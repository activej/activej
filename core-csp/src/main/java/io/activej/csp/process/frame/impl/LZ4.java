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

package io.activej.csp.process.frame.impl;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.ApplicationSettings;
import io.activej.common.Checks;
import io.activej.common.MemSize;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.UnknownFormatException;
import io.activej.csp.binary.Utils;
import io.activej.csp.process.frame.BlockDecoder;
import io.activej.csp.process.frame.BlockEncoder;
import io.activej.csp.process.frame.FrameFormat;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static io.activej.common.Checks.checkArgument;

@ExposedInternals
public final class LZ4 implements FrameFormat {
	public static final boolean CHECKS = Checks.isEnabled(LZ4.class);
	public static final MemSize MAX_BLOCK_SIZE = ApplicationSettings.getMemSize(LZ4.class, "maxBlockSize", MemSize.megabytes(256));

	static final byte[] MAGIC = {'L', 'Z', '4', 1};
	static final byte[] LAST_BLOCK_BYTES = {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
	static final byte[] MAGIC_AND_LAST_BLOCK_BYTES;
	static final int MAGIC_LENGTH = MAGIC.length;

	static {
		MAGIC_AND_LAST_BLOCK_BYTES = new byte[MAGIC.length + LAST_BLOCK_BYTES.length];
		System.arraycopy(MAGIC, 0, MAGIC_AND_LAST_BLOCK_BYTES, 0, MAGIC.length);
		System.arraycopy(LAST_BLOCK_BYTES, 0, MAGIC_AND_LAST_BLOCK_BYTES, MAGIC.length, LAST_BLOCK_BYTES.length);
	}

	static final int COMPRESSED_LENGTH_MASK = 0x7fffffff;
	static final byte END_OF_BLOCK = 1;

	public LZ4Factory factory;
	public int compressionLevel;

	public LZ4(LZ4Factory factory, int compressionLevel) {
		this.factory = factory;
		this.compressionLevel = compressionLevel;
	}

	public static LZ4 create() {
		return builder().build();
	}

	public static Builder builder() {
		return new LZ4(LZ4Factory.fastestInstance(), 0).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, LZ4> {
		private Builder() {}

		public Builder withLZ4Factory(LZ4Factory factory) {
			checkNotBuilt(this);
			LZ4.this.factory = factory;
			return this;
		}

		public Builder withHighCompression() {
			checkNotBuilt(this);
			LZ4.this.compressionLevel = -1;
			return this;
		}

		public Builder withCompressionLevel(int compressionLevel) {
			checkNotBuilt(this);
			checkArgument(compressionLevel >= -1);
			LZ4.this.compressionLevel = compressionLevel;
			return this;
		}

		@Override
		protected LZ4 doBuild() {
			return LZ4.this;
		}
	}

	@Override
	public BlockEncoder createEncoder() {
		LZ4Compressor compressor = compressionLevel == 0 ?
			factory.fastCompressor() :
			compressionLevel == -1 ?
				factory.highCompressor() :
				factory.highCompressor(compressionLevel);
		return new Encoder(compressor);
	}

	@Override
	public BlockDecoder createDecoder() {
		return new Decoder(factory.fastDecompressor());
	}

	public static final class Encoder implements BlockEncoder {

		private final LZ4Compressor compressor;
		private boolean writeHeader = true;

		Encoder(LZ4Compressor compressor) {
			this.compressor = compressor;
		}

		@Override
		public void reset() {
			writeHeader = true;
		}

		@Override
		public ByteBuf encode(ByteBuf inputBuf) {
			int headerSize = writeHeader ? MAGIC_LENGTH : 0;
			writeHeader = false;

			int off = inputBuf.head();
			int len = inputBuf.readRemaining();
			byte[] array = inputBuf.array();

			if (CHECKS) checkArgument(len != 0, "Encoding empty buf");

			ByteBuf outputBuf = ByteBufPool.allocate(headerSize + 2 * 4 + compressor.maxCompressedLength(len) + 1);

			if (headerSize != 0) {
				System.arraycopy(MAGIC, 0, outputBuf.array(), 0, MAGIC_LENGTH);
				outputBuf.moveTail(MAGIC_LENGTH);
			}

			int compressedLength = compressor.compress(array, off, len, outputBuf.array(), headerSize + 2 * 4);

			if (compressedLength + 4 < len) {
				outputBuf.writeInt(compressedLength | ~COMPRESSED_LENGTH_MASK);
				outputBuf.writeInt(len);
				outputBuf.moveTail(compressedLength);
			} else {
				outputBuf.writeInt(len);
				System.arraycopy(array, off, outputBuf.array(), outputBuf.tail(), len);
				outputBuf.moveTail(len);
			}
			outputBuf.put(END_OF_BLOCK);

			return outputBuf;
		}

		@Override
		public ByteBuf encodeEndOfStreamBlock() {
			if (!writeHeader) return ByteBuf.wrapForReading(LAST_BLOCK_BYTES);
			writeHeader = false;
			return ByteBuf.wrapForReading(MAGIC_AND_LAST_BLOCK_BYTES);
		}

	}

	public static final class Decoder implements BlockDecoder {
		private static final int LAST_BLOCK_INT = 0xffffffff;

		private final LZ4FastDecompressor decompressor;
		private boolean readHeader = true;

		private final Utils.IntByteScanner intScanner = new Utils.IntByteScanner();

		Decoder(LZ4FastDecompressor decompressor) {
			this.decompressor = decompressor;
		}

		@Override
		public void reset() {
			readHeader = true;
		}

		@Override
		public @Nullable ByteBuf decode(ByteBufs bufs) throws MalformedDataException {
			if (readHeader) {
				if (!readHeader(bufs)) return null;
				readHeader = false;
			}

			if (bufs.scanBytes(intScanner) == 0) return null;
			int compressedSize = intScanner.getValue();
			if (compressedSize == LAST_BLOCK_INT) {
				bufs.skip(4);
				return END_OF_STREAM;
			}

			if (compressedSize >= 0) {
				if (!bufs.hasRemainingBytes(4 + compressedSize + 1)) return null;
				bufs.skip(4);
				ByteBuf result = bufs.takeExactSize(compressedSize + 1);
				if (result.at(result.tail() - 1) != END_OF_BLOCK) {
					throw new MalformedDataException("Block does not end with special byte '1'");
				}
				result.moveTail(-1);
				return result;
			} else {
				return decompress(bufs, compressedSize & COMPRESSED_LENGTH_MASK);
			}
		}

		@Override
		public boolean ignoreMissingEndOfStreamBlock() {
			return false;
		}

		private boolean readHeader(ByteBufs bufs) throws MalformedDataException {
			return bufs.consumeBytes((index, value) -> {
				if (value != MAGIC[index])
					throw new UnknownFormatException("Expected stream to start with bytes: " + Arrays.toString(MAGIC));
				return index == MAGIC_LENGTH - 1;
			}) != 0;
		}

		private @Nullable ByteBuf decompress(ByteBufs bufs, int compressedSize) throws MalformedDataException {
			if (!bufs.hasRemainingBytes(4 + 4 + compressedSize + 1)) return null;

			bufs.consumeBytes(4, intScanner);
			int originalSize = intScanner.getValue();
			if (originalSize < 0 || originalSize > MAX_BLOCK_SIZE.toInt()) {
				throw new InvalidSizeException(
					"Size (" + originalSize +
					") of block is either negative or exceeds max block size (" + MAX_BLOCK_SIZE + ')');
			}

			ByteBuf firstBuf = bufs.peekBuf();
			assert firstBuf != null; // ensured above

			ByteBuf compressedBuf = firstBuf.readRemaining() >= compressedSize + 1 ? firstBuf : bufs.takeExactSize(compressedSize + 1);

			if (compressedBuf.at(compressedBuf.head() + compressedSize) != END_OF_BLOCK) {
				throw new MalformedDataException("Block does not end with special byte '1'");
			}

			ByteBuf buf = ByteBufPool.allocate(originalSize);
			try {
				int readBytes = decompressor.decompress(compressedBuf.array(), compressedBuf.head(), buf.array(), 0, originalSize);
				if (readBytes != compressedSize) {
					buf.recycle();
					throw new InvalidSizeException("Actual size of decompressed data does not equal expected size of decompressed data");
				}
				buf.tail(originalSize);
			} catch (LZ4Exception e) {
				buf.recycle();
				throw new MalformedDataException("Failed to decompress data", e);
			}

			if (compressedBuf != firstBuf) {
				compressedBuf.recycle();
			} else {
				bufs.skip(compressedSize + 1);
			}

			return buf;
		}
	}
}
