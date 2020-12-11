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
import io.activej.bytebuf.ByteBufQueue.ByteScanner;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;
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

	private ByteBufQueue bufs;
	private BinaryChannelSupplier input;
	private ChannelConsumer<ByteBuf> output;

	int chunkLength;

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
					chunkLength = 0;
					int endIndex = bufs.scanBytes((index, c) -> {
						if (c >= '0' && c <= '9') {
							chunkLength = (chunkLength << 4) + (c - '0');
						} else if (c >= 'a' && c <= 'f') {
							chunkLength = (chunkLength << 4) + (c - 'a' + 10);
						} else if (c >= 'A' && c <= 'F') {
							chunkLength = (chunkLength << 4) + (c - 'A' + 10);
						} else if (c == ';' || c == CR) {
							// Success
							if (index == 0 || chunkLength < 0) {
								throw new InvalidSizeException("Malformed chunk length");
							}
							return true;
						} else {
							throw new InvalidSizeException("Unexpected data");
						}
						if (index == MAX_CHUNK_LENGTH_DIGITS + 1) {
							throw new InvalidSizeException("Chunk length exceeds maximum allowed size");
						}
						return false;
					});
					if (endIndex == -1) return null;
					bufs.skip(endIndex);
					return chunkLength;
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
			try {
				int lastLfIndex = bufs.scanBytes(new ByteScanner() {
					int crlfCrlfSequence;

					@Override
					public boolean consume(int index, byte nextByte) {
						int remainder = nextByte == CR ? 0 : nextByte == LF ? 1 : -1;

						if (crlfCrlfSequence % 2 == remainder) {
							return ++crlfCrlfSequence == 4;
						}
						crlfCrlfSequence = 0;
						return false;
					}
				});
				if (lastLfIndex != -1) {
					bufs.skip(lastLfIndex + 1);
					input.endOfStream()
							.then(output::acceptEndOfStream)
							.whenResult(this::completeProcess);
					return;
				}
			} catch (MalformedDataException ignored) {
				throw new AssertionError("Exceptions cannot be caught here");
			}

			bufs.skip(remainingBytes - 3);
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
