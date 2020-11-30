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
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.parse.InvalidSizeException;
import io.activej.common.exception.parse.ParseException;
import io.activej.common.exception.parse.UnknownFormatException;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelOutput;
import io.activej.csp.binary.BinaryChannelInput;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.binary.ByteBufsDecoder;
import io.activej.csp.dsl.WithBinaryChannelInput;
import io.activej.csp.dsl.WithChannelTransformer;
import io.activej.csp.process.AbstractCommunicatingProcess;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static io.activej.common.Checks.checkState;
import static io.activej.csp.binary.ByteBufsDecoder.ofFixedSize;
import static java.lang.Integer.reverseBytes;
import static java.lang.Math.max;
import static java.lang.Short.reverseBytes;

/**
 * This is a channel transformer, that converts channels of {@link ByteBuf ByteBufs}
 * decompressing the data using the DEFLATE algorithm with standard implementation from the java.util.zip package.
 * <p>
 * It is used in HTTP when {@link io.activej.http.HttpMessage#setBodyGzipCompression HttpMessage#setBodyGzipCompression}
 * method is used.
 */
public final class BufsConsumerGzipInflater extends AbstractCommunicatingProcess
		implements WithChannelTransformer<BufsConsumerGzipInflater, ByteBuf, ByteBuf>, WithBinaryChannelInput<BufsConsumerGzipInflater> {
	public static final int MAX_HEADER_FIELD_LENGTH = 4096; //4 Kb
	public static final int DEFAULT_BUF_SIZE = 512;
	// region exceptions
	private static final ParseException ACTUAL_DECOMPRESSED_DATA_SIZE_IS_NOT_EQUAL_TO_EXPECTED = new InvalidSizeException(BufsConsumerGzipInflater.class, "Decompressed data size is not equal to input size from GZIP trailer");
	private static final ParseException CRC32_VALUE_DIFFERS = new ParseException(BufsConsumerGzipInflater.class, "CRC32 value of uncompressed data differs");
	private static final ParseException INCORRECT_ID_HEADER_BYTES = new UnknownFormatException(BufsConsumerGzipInflater.class, "Incorrect identification bytes. Not in GZIP format");
	private static final ParseException UNSUPPORTED_COMPRESSION_METHOD = new UnknownFormatException(BufsConsumerGzipInflater.class, "Unsupported compression method. Deflate compression required");
	private static final ParseException FEXTRA_TOO_LARGE = new InvalidSizeException(BufsConsumerGzipInflater.class, "FEXTRA part of a header is larger than maximum allowed length");
	private static final ParseException FNAME_FCOMMENT_TOO_LARGE = new InvalidSizeException(BufsConsumerGzipInflater.class, "FNAME or FEXTRA header is larger than maximum allowed length");
	private static final ParseException MALFORMED_FLAG = new ParseException(BufsConsumerGzipInflater.class, "Flag byte of a header is malformed. Reserved bits are set");
	// endregion
	// rfc 1952 section 2.3.1
	private static final byte[] GZIP_HEADER = {(byte) 0x1f, (byte) 0x8b, Deflater.DEFLATED};
	private static final int GZIP_FOOTER_SIZE = 8;
	private static final int FHCRC = 2;
	private static final int FEXTRA = 4;
	private static final int FNAME = 8;
	private static final int FCOMMENT = 16;

	private final CRC32 crc32 = new CRC32();

	private Inflater inflater = new Inflater(true);

	private ByteBufQueue bufs;
	private BinaryChannelSupplier input;
	private ChannelConsumer<ByteBuf> output;

	// region creators
	private BufsConsumerGzipInflater() {
	}

	public static BufsConsumerGzipInflater create() {
		return new BufsConsumerGzipInflater();
	}

	public BufsConsumerGzipInflater withInflater(@NotNull Inflater inflater) {
		this.inflater = inflater;
		return this;
	}

	@Override
	public BinaryChannelInput getInput() {
		return input -> {
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
			checkState(this.output == null, "Output already set");
			this.output = sanitize(output);
			if (this.input != null && this.output != null) {
				startProcess();
			}
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
		processHeader();
	}

	private void processHeader() {
		input.parse(ofFixedSize(10))
				.whenResult(buf -> {
					//header validation
					if (buf.get() != GZIP_HEADER[0] || buf.get() != GZIP_HEADER[1]) {
						buf.recycle();
						closeEx(INCORRECT_ID_HEADER_BYTES);
						return;
					}
					if (buf.get() != GZIP_HEADER[2]) {
						buf.recycle();
						closeEx(UNSUPPORTED_COMPRESSION_METHOD);
						return;
					}

					byte flag = buf.get();
					if ((flag & 0b11100000) > 0) {
						buf.recycle();
						closeEx(MALFORMED_FLAG);
						return;
					}
					// unsetting FTEXT bit
					flag &= ~1;
					buf.recycle();
					runNext(flag).run();
				})
				.whenException(this::closeEx);
	}

	private void processBody() {
		ByteBufQueue queue = new ByteBufQueue();

		while (bufs.hasRemaining()) {
			ByteBuf src = bufs.peekBuf();
			assert src != null; // hasRemaining() succeeded above
			inflater.setInput(src.array(), src.head(), src.readRemaining());
			try {
				inflate(queue, src);
			} catch (DataFormatException e) {
				queue.recycle();
				closeEx(e);
				return;
			}
			if (inflater.finished()) {
				output.acceptAll(queue.asIterator())
						.whenResult(this::processFooter);
				return;
			}
		}
		output.acceptAll(queue.asIterator())
				.then(() -> input.needMoreData())
				.whenResult(this::processBody);
	}

	private void processFooter() {
		input.parse(ofFixedSize(GZIP_FOOTER_SIZE))
				.whenResult(buf -> {
					if ((int) crc32.getValue() != reverseBytes(buf.readInt())) {
						closeEx(CRC32_VALUE_DIFFERS);
						buf.recycle();
						return;
					}
					if (inflater.getTotalOut() != reverseBytes(buf.readInt())) {
						closeEx(ACTUAL_DECOMPRESSED_DATA_SIZE_IS_NOT_EQUAL_TO_EXPECTED);
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

	private void inflate(ByteBufQueue queue, ByteBuf src) throws DataFormatException {
		while (true) {
			ByteBuf buf = ByteBufPool.allocate(max(src.readRemaining(), DEFAULT_BUF_SIZE));
			int beforeInflation = inflater.getTotalIn();
			int len = inflater.inflate(buf.array(), 0, buf.writeRemaining());
			buf.moveTail(len);
			src.moveHead(inflater.getTotalIn() - beforeInflation);
			if (len == 0) {
				if (!src.canRead()) {
					bufs.take().recycle();
				}
				buf.recycle();
				return;
			}
			crc32.update(buf.array(), buf.head(), buf.readRemaining());
			queue.add(buf);
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
		input.parse(ByteBufsDecoder.ofNullTerminatedBytes(MAX_HEADER_FIELD_LENGTH))
				.whenException(e -> closeEx(FNAME_FCOMMENT_TOO_LARGE))
				.whenResult(ByteBuf::recycle)
				.whenResult(() -> runNext(flag - part).run());
	}

	private void skipExtra(int flag) {
		input.parse(ofFixedSize(2))
				.map(shortBuf -> {
					short toSkip = reverseBytes(shortBuf.readShort());
					shortBuf.recycle();
					return toSkip;
				})
				.then(toSkip -> {
					if (toSkip > MAX_HEADER_FIELD_LENGTH) {
						closeEx(FEXTRA_TOO_LARGE);
						return Promise.ofException(FEXTRA_TOO_LARGE);
					}
					return input.parse(ofFixedSize(toSkip));
				})
				.whenException(this::closeEx)
				.whenResult(ByteBuf::recycle)
				.whenResult(() -> runNext(flag - FEXTRA).run());
	}

	private void skipCRC16(int flag) {
		input.parse(ofFixedSize(2))
				.whenException(this::closeEx)
				.whenResult(ByteBuf::recycle)
				.whenResult(() -> runNext(flag - FHCRC).run());
	}

	private Runnable runNext(int flag) {
		if (flag != 0) {
			return () -> skipHeaders(flag);
		}
		return this::processBody;
	}
	// endregion

	@Override
	protected void doClose(Throwable e) {
		inflater.end();
		input.closeEx(e);
		output.closeEx(e);
	}
}
