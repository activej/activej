/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.http.stream;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.parse.InvalidSizeException;
import io.activej.common.exception.parse.ParseException;
import io.activej.common.ref.RefInt;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelOutput;
import io.activej.csp.binary.BinaryChannelInput;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.dsl.WithBinaryChannelInput;
import io.activej.csp.dsl.WithChannelTransformer;
import io.activej.csp.process.AbstractCommunicatingProcess;
import io.activej.promise.Promise;

import static io.activej.bytebuf.ByteBufStrings.CR;
import static io.activej.bytebuf.ByteBufStrings.LF;
import static io.activej.common.Checks.checkState;
import static io.activej.csp.binary.ByteBufsDecoder.assertBytes;
import static io.activej.csp.binary.ByteBufsDecoder.ofCrlfTerminatedBytes;

/**
 * This is a binary channel transformer, that converts channels of {@link ByteBuf ByteBufs}
 * converting <a href="https://tools.ietf.org/html/rfc2616#section-3.6.1">chunked transfer encoding<a>
 * data into its raw form.
 */
public final class BufsConsumerChunkedDecoder extends AbstractCommunicatingProcess
		implements WithChannelTransformer<BufsConsumerChunkedDecoder, ByteBuf, ByteBuf>, WithBinaryChannelInput<BufsConsumerChunkedDecoder> {
	public static final int MAX_CHUNK_LENGTH_DIGITS = 8;
	public static final byte[] CRLF = {13, 10};
	// region exceptions
	public static final ParseException MALFORMED_CHUNK = new ParseException(BufsConsumerChunkedDecoder.class, "Malformed chunk");
	public static final ParseException MALFORMED_CHUNK_LENGTH = new InvalidSizeException(BufsConsumerChunkedDecoder.class, "Malformed chunk length");
	// endregion

	private ByteBufQueue bufs;
	private BinaryChannelSupplier input;
	private ChannelConsumer<ByteBuf> output;

	// region creators
	private BufsConsumerChunkedDecoder() {
	}

	public static BufsConsumerChunkedDecoder create() {
		return new BufsConsumerChunkedDecoder();
	}

	@Override
	public BinaryChannelInput getInput() {
		return input -> {
			checkState(this.input == null, "Input already set");
			this.input = sanitize(input);
			this.bufs = input.getBufs();
			if (this.input != null && this.output != null) startProcess();
			return getProcessCompletion();
		};
	}

	@SuppressWarnings("ConstantConditions") //check output for clarity
	@Override
	public ChannelOutput<ByteBuf> getOutput() {
		return output -> {
			checkState(this.output == null, "Output already set");
			this.output = sanitize(output);
			if (this.input != null && this.output != null) startProcess();
		};
	}
	// endregion

	@Override
	protected void beforeProcess() {
		checkState(input != null, "Input was not set");
		checkState(output != null, "Output was not set");
	}

	@Override
	protected void doProcess() {
		processLength();
	}

	private void processLength() {
		input.parse(
				queue -> {
					RefInt chunkLength = new RefInt(0);
					RefInt index = new RefInt(0);

					int lastIndex = bufs.scanBytes(c -> {
						if (c >= '0' && c <= '9') {
							chunkLength.set((chunkLength.get() << 4) + (c - '0'));
						} else if (c >= 'a' && c <= 'f') {
							chunkLength.set((chunkLength.get() << 4) + (c - 'a' + 10));
						} else if (c >= 'A' && c <= 'F') {
							chunkLength.set((chunkLength.get() << 4) + (c - 'A' + 10));
						} else if (c == ';' || c == CR) {
							// Success
							return true;
						} else {
							chunkLength.set(-1);
							return true;
						}
						return index.inc() == MAX_CHUNK_LENGTH_DIGITS + 1;
					});

					if (lastIndex == -1) return null;
					if (index.get() == 0 || index.get() == MAX_CHUNK_LENGTH_DIGITS + 1 || chunkLength.get() < 0) {
						throw MALFORMED_CHUNK_LENGTH;
					}

					queue.skip(index.get());
					return chunkLength.get();
				})
				.whenException(this::closeEx)
				.whenResult(chunkLength -> {
					if (chunkLength != 0) {
						consumeCRLF(chunkLength);
					} else {
						validateLastChunk();
					}
				});
	}

	private void processData(int chunkLength) {
		ByteBuf buf = bufs.takeAtMost(chunkLength);
		int newChunkLength = chunkLength - buf.readRemaining();
		if (newChunkLength != 0) {
			Promise.complete()
					.then(() -> buf.canRead() ? output.accept(buf) : Promise.complete())
					.then(() -> bufs.isEmpty() ? input.needMoreData() : Promise.complete())
					.whenResult(() -> processData(newChunkLength));
			return;
		}
		input.parse(assertBytes(CRLF))
				.whenException(buf::recycle)
				.then(() -> output.accept(buf))
				.whenResult(this::processLength);
	}

	private void consumeCRLF(int chunkLength) {
		input.parse(
				bufs -> {
					ByteBuf maybeResult = ofCrlfTerminatedBytes().tryDecode(bufs);
					if (maybeResult == null) {
						bufs.skip(bufs.remainingBytes() - 1);
					}
					return maybeResult;
				})
				.whenResult(ByteBuf::recycle)
				.whenException(this::closeEx)
				.whenResult(() -> processData(chunkLength));
	}

	private void validateLastChunk() {
		int remainingBytes = bufs.remainingBytes();
		if (remainingBytes >= 4) {
			RefInt seqCount = new RefInt(0); // CR LF CR LF
			int lastLfIndex = bufs.scanBytes(nextByte -> {
				int remainder = nextByte == CR ? 0 : nextByte == LF ? 1 : -1;

				if (seqCount.value % 2 == remainder) {
					seqCount.inc();
				} else {
					seqCount.set(0);
				}
				return seqCount.value == 4;
			});

			if (lastLfIndex == -1) {
				bufs.skip(remainingBytes - 3);
			} else {
				bufs.skip(lastLfIndex + 1);
				input.endOfStream()
						.then(output::acceptEndOfStream)
						.whenResult(this::completeProcess);
				return;
			}
		}
		input.needMoreData()
				.whenResult(this::validateLastChunk);
	}

	@Override
	protected void doClose(Throwable e) {
		input.closeEx(e);
		output.closeEx(e);
	}
}
