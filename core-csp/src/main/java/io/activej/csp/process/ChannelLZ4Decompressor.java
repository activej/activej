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

package io.activej.csp.process;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.inspector.BaseInspector;
import io.activej.common.parse.ParseException;
import io.activej.common.parse.TruncatedDataException;
import io.activej.csp.AbstractCommunicatingProcess;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelOutput;
import io.activej.csp.binary.BinaryChannelInput;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.dsl.WithBinaryChannelInput;
import io.activej.csp.dsl.WithChannelTransformer;
import io.activej.promise.Promise;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.SafeUtils;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.jetbrains.annotations.Nullable;

import static io.activej.csp.binary.BinaryChannelSupplier.UNEXPECTED_END_OF_STREAM_EXCEPTION;
import static io.activej.csp.process.ChannelLZ4Compressor.*;
import static java.lang.Math.min;

public final class ChannelLZ4Decompressor extends AbstractCommunicatingProcess
		implements WithChannelTransformer<ChannelLZ4Decompressor, ByteBuf, ByteBuf>, WithBinaryChannelInput<ChannelLZ4Decompressor> {
	public static final int HEADER_LENGTH = ChannelLZ4Compressor.HEADER_LENGTH;
	public static final ParseException STREAM_IS_CORRUPTED = new ParseException(ChannelLZ4Decompressor.class, "Stream is corrupted");

	private final LZ4FastDecompressor decompressor;
	private final StreamingXXHash32 checksum;

	private ByteBufQueue bufs;
	private BinaryChannelSupplier input;
	private ChannelConsumer<ByteBuf> output;

	private final Header header = new Header();

	@Nullable
	private Inspector inspector;

	public interface Inspector extends BaseInspector<Inspector> {
		void onBlock(ChannelLZ4Decompressor self, Header header, ByteBuf inputBuf, ByteBuf outputBuf);
	}

	public abstract static class ForwardingInspector implements Inspector {
		protected final @Nullable Inspector next;

		public ForwardingInspector(@Nullable Inspector next) {this.next = next;}

		@Override
		public void onBlock(ChannelLZ4Decompressor self, Header header, ByteBuf inputBuf, ByteBuf outputBuf) {
			if (next != null) next.onBlock(self, header, inputBuf, outputBuf);
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T extends Inspector> @Nullable T lookup(Class<T> type) {
			return type.isAssignableFrom(this.getClass()) ? (T) this : next != null ? next.lookup(type) : null;
		}
	}

	// region creators
	private ChannelLZ4Decompressor(LZ4FastDecompressor decompressor, StreamingXXHash32 checksum) {
		this.decompressor = decompressor;
		this.checksum = checksum;
	}

	public static ChannelLZ4Decompressor create() {
		return create(
				LZ4Factory.fastestInstance().fastDecompressor(),
				XXHashFactory.fastestInstance());
	}

	public static ChannelLZ4Decompressor create(LZ4FastDecompressor decompressor, XXHashFactory xxHashFactory) {
		return new ChannelLZ4Decompressor(decompressor, xxHashFactory.newStreamingHash32(DEFAULT_SEED));
	}

	public ChannelLZ4Decompressor withInspector(Inspector inspector) {
		this.inspector = inspector;
		return this;
	}

	@Override
	public BinaryChannelInput getInput() {
		return input -> {
			this.input = input;
			this.bufs = input.getBufs();
			if (this.input != null && this.output != null) startProcess();
			return getProcessCompletion();
		};
	}

	@SuppressWarnings("ConstantConditions") //check output for clarity
	@Override
	public ChannelOutput<ByteBuf> getOutput() {
		return output -> {
			this.output = sanitize(output);
			if (this.input != null && this.output != null) startProcess();
		};
	}

	// endregion

	@Override
	protected void doProcess() {
		processHeader();
	}

	public void processHeader() {
		if (!bufs.hasRemainingBytes(HEADER_LENGTH)) {
			for (int i = 0; i < min(bufs.remainingBytes(), MAGIC.length); i++) {
				if (bufs.peekByte(i) != MAGIC[i]) {
					closeEx(STREAM_IS_CORRUPTED);
					return;
				}
			}
			input.needMoreData()
					.thenEx(ChannelLZ4Decompressor::checkTruncatedDataException)
					.thenEx(this::sanitize)
					.whenResult(this::processHeader);
			return;
		}

		try (ByteBuf headerBuf = bufs.takeExactSize(HEADER_LENGTH)) {
			readHeader(header, headerBuf.array(), headerBuf.head());
		} catch (ParseException e) {
			closeEx(e);
			return;
		}

		if (!header.finished) {
			processBody();
			return;
		}

		input.endOfStream()
				.thenEx(this::sanitize)
				.then(output::acceptEndOfStream)
				.whenResult(this::completeProcess);
	}

	public void processBody() {
		if (!bufs.hasRemainingBytes(header.compressedLen)) {
			input.needMoreData()
					.thenEx(ChannelLZ4Decompressor::checkTruncatedDataException)
					.thenEx(this::sanitize)
					.whenResult(this::processBody);
			return;
		}

		ByteBuf inputBuf = bufs.takeExactSize(header.compressedLen);
		ByteBuf outputBuf;
		try {
			outputBuf = decompress(decompressor, checksum, header, inputBuf.array(), inputBuf.head());
			if (inspector != null) inspector.onBlock(this, header, inputBuf, outputBuf);
		} catch (ParseException e) {
			closeEx(e);
			return;
		} finally {
			inputBuf.recycle();
		}

		output.accept(outputBuf)
				.whenResult(this::processHeader);
	}

	@Override
	protected void doClose(Throwable e) {
		input.closeEx(e);
		output.closeEx(e);
	}

	public static final class Header {
		public int originalLen;
		public int compressedLen;
		public int compressionMethod;
		public int check;
		public boolean finished;
	}

	private static void readHeader(Header header, byte[] buf, int off) throws ParseException {
		for (int i = 0; i < MAGIC_LENGTH; ++i) {
			if (buf[off + i] != MAGIC[i]) {
				throw STREAM_IS_CORRUPTED;
			}
		}
		int token = buf[off + MAGIC_LENGTH] & 0xFF;
		header.compressionMethod = token & 0xF0;
		int compressionLevel = COMPRESSION_LEVEL_BASE + (token & 0x0F);
		if (header.compressionMethod != COMPRESSION_METHOD_RAW && header.compressionMethod != COMPRESSION_METHOD_LZ4) {
			throw STREAM_IS_CORRUPTED;
		}
		header.compressedLen = SafeUtils.readIntLE(buf, off + MAGIC_LENGTH + 1);
		header.originalLen = SafeUtils.readIntLE(buf, off + MAGIC_LENGTH + 5);
		header.check = SafeUtils.readIntLE(buf, off + MAGIC_LENGTH + 9);
		if (header.originalLen > 1 << compressionLevel
				|| (header.originalLen < 0 || header.compressedLen < 0)
				|| (header.originalLen == 0 && header.compressedLen != 0)
				|| (header.originalLen != 0 && header.compressedLen == 0)
				|| (header.compressionMethod == COMPRESSION_METHOD_RAW && header.originalLen != header.compressedLen)) {
			throw STREAM_IS_CORRUPTED;
		}
		if (header.originalLen == 0) {
			if (header.check != 0) {
				throw STREAM_IS_CORRUPTED;
			}
			header.finished = true;
		}
	}

	private static ByteBuf decompress(LZ4FastDecompressor decompressor, StreamingXXHash32 checksum, Header header,
			byte[] bytes, int off) throws ParseException {
		ByteBuf outputBuf = ByteBufPool.allocate(header.originalLen);
		outputBuf.tail(header.originalLen);
		switch (header.compressionMethod) {
			case COMPRESSION_METHOD_RAW:
				System.arraycopy(bytes, off, outputBuf.array(), 0, header.originalLen);
				break;
			case COMPRESSION_METHOD_LZ4:
				try {
					int compressedLen2 = decompressor.decompress(bytes, off, outputBuf.array(), 0, header.originalLen);
					if (header.compressedLen != compressedLen2) {
						throw STREAM_IS_CORRUPTED;
					}
				} catch (LZ4Exception e) {
					throw new ParseException(ChannelLZ4Decompressor.class, "Stream is corrupted", e);
				}
				break;
			default:
				throw STREAM_IS_CORRUPTED;
		}
		checksum.reset();
		checksum.update(outputBuf.array(), 0, header.originalLen);
		if (checksum.getValue() != header.check) {
			throw STREAM_IS_CORRUPTED;
		}
		return outputBuf;
	}

	private static Promise<Void> checkTruncatedDataException(Void $, Throwable e) {
		if (e == null) {
			return Promise.complete();
		} else {
			if (e == UNEXPECTED_END_OF_STREAM_EXCEPTION) {
				return Promise.ofException(new TruncatedDataException(ChannelLZ4Decompressor.class, "Unexpected end-of-stream"));
			} else {
				return Promise.ofException(e);
			}
		}
	}

}
