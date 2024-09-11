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
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.ApplicationSettings;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.TruncatedDataException;
import io.activej.common.exception.UnknownFormatException;
import io.activej.csp.ChannelOutput;
import io.activej.csp.binary.BinaryChannelInput;
import io.activej.csp.binary.decoder.ByteBufsDecoders;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.csp.dsl.WithBinaryChannelInput;
import io.activej.csp.dsl.WithChannelTransformer;
import io.activej.csp.process.AbstractCommunicatingProcess;
import io.activej.promise.Promise;

import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static io.activej.common.Checks.checkState;
import static io.activej.csp.binary.decoder.ByteBufsDecoders.ofFixedSize;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.lang.Integer.reverseBytes;
import static java.lang.Math.max;
import static java.lang.Short.reverseBytes;

/**
 * This is a channel transformer, that converts channels of {@link ByteBuf ByteBufs}
 * decompressing the data using the DEFLATE algorithm with standard implementation from the java.util.zip package.
 * <p>
 * It is used in HTTP when {@link io.activej.http.HttpMessage.Builder#withBodyGzipCompression HttpMessage.Builder#withBodyGzipCompression}
 * method is used.
 */
public final class BufsConsumerGzipInflater extends AbstractCommunicatingProcess
	implements WithChannelTransformer<BufsConsumerGzipInflater, ByteBuf, ByteBuf>, WithBinaryChannelInput<BufsConsumerGzipInflater> {
	public static final int MAX_HEADER_FIELD_LENGTH = 4096; //4 Kb
	public static final int DEFAULT_BUF_SIZE = ApplicationSettings.getInt(BufsConsumerGzipInflater.class, "bufferSize", 512);
	// rfc 1952 section 2.3.1
	private static final byte[] GZIP_HEADER = {(byte) 0x1f, (byte) 0x8b, Deflater.DEFLATED};
	private static final int GZIP_FOOTER_SIZE = 8;
	private static final int FHCRC = 2;
	private static final int FEXTRA = 4;
	private static final int FNAME = 8;
	private static final int FCOMMENT = 16;

	private final CRC32 crc32 = new CRC32();

	private Inflater inflater = new Inflater(true);

	private ByteBufs bufs;
	private SanitizedBinaryChannelSupplier input;
	private ChannelConsumer<ByteBuf> output;

	private BufsConsumerGzipInflater() {}

	public static BufsConsumerGzipInflater create() {
		return builder().build();
	}

	public static Builder builder() {
		return new BufsConsumerGzipInflater().new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, BufsConsumerGzipInflater> {
		private Builder() {}

		public Builder withInflater(Inflater inflater) {
			checkNotBuilt(this);
			BufsConsumerGzipInflater.this.inflater = inflater;
			return this;
		}

		@Override
		protected BufsConsumerGzipInflater doBuild() {
			return BufsConsumerGzipInflater.this;
		}
	}

	@Override
	public BinaryChannelInput getInput() {
		return input -> {
			checkInReactorThread(this);
			checkState(this.input == null, "Input already set");
			this.input = sanitize(input);
			this.bufs = input.getBufs();
			if (this.input != null && this.output != null) {
				startProcess();
			}
			return getProcessCompletion();
		};
	}

	@SuppressWarnings("ConstantConditions") //check output for clarity
	@Override
	public ChannelOutput<ByteBuf> getOutput() {
		return output -> {
			checkInReactorThread(this);
			checkState(this.output == null, "Output already set");
			this.output = sanitize(output);
			if (this.input != null && this.output != null) {
				startProcess();
			}
		};
	}

	@Override
	protected void beforeProcess() {
		checkState(input != null, "Input was not set");
		checkState(output != null, "Output was not set");
	}

	@Override
	protected void doProcess() {
		processHeader();
	}

	private void processHeader() {
		if (input.getBufs().isEmpty()) {
			input.getUnsanitizedSupplier().needMoreData()
				.whenResult(this::processHeader)
				.whenException(e -> {
					if (e instanceof TruncatedDataException) {
						output.acceptEndOfStream()
							.whenResult(this::completeProcess);
					} else {
						closeEx(e);
					}
				});
			return;
		}
		input.decode(ofFixedSize(10))
			.whenResult(buf -> {
				//header validation
				if (buf.get() != GZIP_HEADER[0] || buf.get() != GZIP_HEADER[1]) {
					buf.recycle();
					closeEx(new UnknownFormatException("Incorrect identification bytes. Not in GZIP format"));
					return;
				}
				if (buf.get() != GZIP_HEADER[2]) {
					buf.recycle();
					closeEx(new UnknownFormatException("Unsupported compression method. Deflate compression required"));
					return;
				}

				byte flag = buf.get();
				if ((flag & 0b11100000) > 0) {
					buf.recycle();
					closeEx(new MalformedDataException("Flag byte of a header is malformed. Reserved bits are set"));
					return;
				}
				// unsetting FTEXT bit
				flag &= ~1;
				buf.recycle();
				runNext(flag);
			})
			.whenException(this::closeEx);
	}

	private void processBody() {
		ByteBufs bufs = new ByteBufs();

		while (this.bufs.hasRemaining()) {
			ByteBuf src = this.bufs.peekBuf();
			assert src != null; // hasRemaining() succeeded above
			inflater.setInput(src.array(), src.head(), src.readRemaining());
			try {
				inflate(bufs, src);
			} catch (DataFormatException e) {
				bufs.recycle();
				closeEx(e);
				return;
			}
			if (inflater.finished()) {
				output.acceptAll(bufs.asIterator())
					.whenResult(this::processFooter);
				return;
			}
		}
		output.acceptAll(bufs.asIterator())
			.then(() -> input.needMoreData())
			.whenResult(this::processBody);
	}

	private void processFooter() {
		input.decode(ofFixedSize(GZIP_FOOTER_SIZE))
			.whenResult(buf -> {
				if ((int) crc32.getValue() != reverseBytes(buf.readInt())) {
					closeEx(new MalformedDataException("CRC32 value of uncompressed data differs"));
					buf.recycle();
					return;
				}
				if (inflater.getTotalOut() != reverseBytes(buf.readInt())) {
					closeEx(new InvalidSizeException("Decompressed data size is not equal to input size from GZIP trailer"));
					buf.recycle();
					return;
				}
				buf.recycle();
				input.endOfStream()
					.then(output::acceptEndOfStream)
					.whenResult(this::completeProcess);
			})
			.whenException(this::closeEx);
	}

	private void inflate(ByteBufs bufs, ByteBuf src) throws DataFormatException {
		while (true) {
			ByteBuf buf = ByteBufPool.allocate(max(src.readRemaining() * 2, DEFAULT_BUF_SIZE));
			int beforeInflation = inflater.getTotalIn();
			int len = inflater.inflate(buf.array(), 0, buf.writeRemaining());
			buf.moveTail(len);
			src.moveHead(inflater.getTotalIn() - beforeInflation);
			if (len == 0) {
				if (!src.canRead()) {
					this.bufs.take().recycle();
				}
				buf.recycle();
				return;
			}
			crc32.update(buf.array(), buf.head(), buf.readRemaining());
			bufs.add(buf);
		}
	}

	// region skip header fields
	private void skipHeaders(int flag) {
		// trying to skip optional gzip file members if any is present
		if ((flag & FEXTRA) != 0) {
			skipExtra(flag);
		} else if ((flag & FNAME) != 0) {
			skipTerminatorByte(flag, FNAME);
		} else if ((flag & FCOMMENT) != 0) {
			skipTerminatorByte(flag, FCOMMENT);
		} else if ((flag & FHCRC) != 0) {
			skipCRC16(flag);
		}
	}

	private void skipTerminatorByte(int flag, int part) {
		input.decode(ByteBufsDecoders.ofNullTerminatedBytes(MAX_HEADER_FIELD_LENGTH))
			.whenException(e -> closeEx(new InvalidSizeException("FNAME or FEXTRA header is larger than maximum allowed length")))
			.whenResult(ByteBuf::recycle)
			.whenResult(() -> runNext(flag - part));
	}

	private void skipExtra(int flag) {
		input.decode(ofFixedSize(2))
			.map(shortBuf -> {
				short toSkip = reverseBytes(shortBuf.readShort());
				shortBuf.recycle();
				return toSkip;
			})
			.then(toSkip -> {
				if (toSkip > MAX_HEADER_FIELD_LENGTH) {
					MalformedDataException exception = new InvalidSizeException("FEXTRA part of a header is larger than maximum allowed length");
					closeEx(exception);
					return Promise.ofException(exception);
				}
				return input.decode(ofFixedSize(toSkip));
			})
			.whenException(this::closeEx)
			.whenResult(ByteBuf::recycle)
			.whenResult(() -> runNext(flag - FEXTRA));
	}

	private void skipCRC16(int flag) {
		input.decode(ofFixedSize(2))
			.whenException(this::closeEx)
			.whenResult(ByteBuf::recycle)
			.whenResult(() -> runNext(flag - FHCRC));
	}

	private void runNext(int flag) {
		if (flag != 0) {
			skipHeaders(flag);
		} else {
			processBody();
		}
	}
	// endregion

	@Override
	protected void doClose(Exception e) {
		inflater.end();
		input.closeEx(e);
		output.closeEx(e);
	}
}
