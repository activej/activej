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
import io.activej.common.Checks;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.UnknownFormatException;
import io.activej.csp.process.frame.BlockDecoder;
import io.activej.csp.process.frame.BlockEncoder;
import io.activej.csp.process.frame.FrameFormat;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.zip.Checksum;

import static io.activej.common.Checks.checkArgument;
import static java.lang.Math.max;
import static java.lang.Math.min;

@Deprecated
@ExposedInternals
public final class LZ4Legacy implements FrameFormat {
	static final byte[] MAGIC = {'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k'};
	static final byte[] LAST_BYTES;
	static final byte[] MAGIC_AND_LAST_BYTES;
	static final int MAGIC_LENGTH = MAGIC.length;

	static final int COMPRESSION_LEVEL_BASE = 10;

	static final int COMPRESSION_METHOD_RAW = 0x10;
	static final int COMPRESSION_METHOD_LZ4 = 0x20;

	static {
		LAST_BYTES = new byte[13];
		LAST_BYTES[0] = COMPRESSION_METHOD_RAW;

		MAGIC_AND_LAST_BYTES = new byte[MAGIC.length + LAST_BYTES.length];
		System.arraycopy(MAGIC, 0, MAGIC_AND_LAST_BYTES, 0, MAGIC.length);
		MAGIC_AND_LAST_BYTES[MAGIC.length] = COMPRESSION_METHOD_RAW;
	}

	static final int HEADER_LENGTH =
		MAGIC.length // magic bytes
		+ 1 // token
		+ 4 // compressed length
		+ 4 // decompressed length
		+ 4; // checksum

	static final int DEFAULT_SEED = 0x9747b28c;

	public LZ4Factory lz4Factory;
	public XXHashFactory hashFactory;
	public boolean legacyChecksum;

	public int compressionLevel;

	public boolean ignoreMissingEndOfStreamBlock;

	public LZ4Legacy(
		LZ4Factory factory, XXHashFactory hashFactory, boolean legacyChecksum, int compressionLevel,
		boolean ignoreMissingEndOfStreamBlock
	) {
		this.lz4Factory = factory;
		this.hashFactory = hashFactory;
		this.legacyChecksum = legacyChecksum;
		this.compressionLevel = compressionLevel;
		this.ignoreMissingEndOfStreamBlock = ignoreMissingEndOfStreamBlock;
	}

	public static LZ4Legacy create() {
		return builder().build();
	}

	public static Builder builder() {
		return new LZ4Legacy(LZ4Factory.fastestInstance(), XXHashFactory.fastestInstance(), false, 0, false).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, LZ4Legacy> {
		private Builder() {}

		public Builder withLZ4Factory(LZ4Factory factory) {
			checkNotBuilt(this);
			LZ4Legacy.this.lz4Factory = factory;
			return this;
		}

		public Builder withHashFactory(XXHashFactory factory) {
			checkNotBuilt(this);
			LZ4Legacy.this.hashFactory = factory;
			return this;
		}

		public Builder withHighCompression() {
			checkNotBuilt(this);
			LZ4Legacy.this.compressionLevel = -1;
			return this;
		}

		public Builder withCompressionLevel(int compressionLevel) {
			checkNotBuilt(this);
			checkArgument(compressionLevel >= -1);
			LZ4Legacy.this.compressionLevel = compressionLevel;
			return this;
		}

		public Builder withIgnoreMissingEndOfStream(boolean ignore) {
			checkNotBuilt(this);
			LZ4Legacy.this.ignoreMissingEndOfStreamBlock = ignore;
			return this;
		}

		/**
		 * Whether streaming hash will be used as a {@link Checksum}, same as in LZ4 library stream encoder/decoder
		 * <p>
		 * Useful for interoperation with {@link net.jpountz.lz4.LZ4BlockOutputStream} and {@link net.jpountz.lz4.LZ4BlockInputStream}
		 */
		@Deprecated
		public Builder withLegacyChecksum(boolean legacyChecksum) {
			checkNotBuilt(this);
			LZ4Legacy.this.legacyChecksum = legacyChecksum;
			return this;
		}

		@Override
		protected LZ4Legacy doBuild() {
			return LZ4Legacy.this;
		}
	}

	@Override
	public BlockEncoder createEncoder() {
		LZ4Compressor compressor = compressionLevel == 0 ?
			lz4Factory.fastCompressor() :
			compressionLevel == -1 ?
				lz4Factory.highCompressor() :
				lz4Factory.highCompressor(compressionLevel);
		StreamingXXHash32 hash = hashFactory.newStreamingHash32(DEFAULT_SEED);
		Checksum checksum = legacyChecksum ? hash.asChecksum() : toSimpleChecksum(hash);
		return new Encoder(compressor, checksum);
	}

	@Override
	public BlockDecoder createDecoder() {
		StreamingXXHash32 hash = hashFactory.newStreamingHash32(DEFAULT_SEED);
		Checksum checksum = legacyChecksum ? hash.asChecksum() : toSimpleChecksum(hash);
		return new Decoder(lz4Factory.fastDecompressor(), checksum, ignoreMissingEndOfStreamBlock);
	}

	private static Checksum toSimpleChecksum(StreamingXXHash32 hash) {
		return new Checksum() {
			@Override
			public void update(int b) {
				hash.update(new byte[]{(byte) b}, 0, 1);
			}

			@Override
			public void update(byte[] b, int off, int len) {
				hash.update(b, off, len);
			}

			@Override
			public long getValue() {
				return hash.getValue();
			}

			@Override
			public void reset() {
				hash.reset();
			}
		};
	}

	@Deprecated
	public static final class Encoder implements BlockEncoder {
		private static final boolean CHECKS = Checks.isEnabled(Encoder.class);

		private static final int MIN_BLOCK_SIZE = 64;
		private static final int MAX_BLOCK_SIZE = 1 << (COMPRESSION_LEVEL_BASE + 0x0F);

		private final LZ4Compressor compressor;
		private final Checksum checksum;

		Encoder(LZ4Compressor compressor, Checksum checksum) {
			this.compressor = compressor;
			this.checksum = checksum;
		}

		@Override
		public void reset() {
		}

		@Override
		public ByteBuf encode(ByteBuf inputBuf) {
			int len = inputBuf.readRemaining();
			if (CHECKS) checkArgument(len != 0);

			int off = inputBuf.head();
			byte[] bytes = inputBuf.array();

			int compressionLevel = compressionLevel(min(max(len, MIN_BLOCK_SIZE), MAX_BLOCK_SIZE));

			int outputBufMaxSize = HEADER_LENGTH + compressor.maxCompressedLength(len);
			ByteBuf outputBuf = ByteBufPool.allocate(outputBufMaxSize);
			outputBuf.put(MAGIC);

			byte[] outputBytes = outputBuf.array();

			checksum.reset();
			checksum.update(bytes, off, len);
			int check = (int) checksum.getValue();

			int compressedLength = compressor.compress(bytes, off, len, outputBytes, HEADER_LENGTH);

			int compressMethod;
			if (compressedLength >= len) {
				compressMethod = COMPRESSION_METHOD_RAW;
				compressedLength = len;
				System.arraycopy(bytes, off, outputBytes, HEADER_LENGTH, len);
			} else {
				compressMethod = COMPRESSION_METHOD_LZ4;
			}

			outputBytes[MAGIC_LENGTH] = (byte) (compressMethod | compressionLevel);
			writeIntLE(compressedLength, outputBytes, MAGIC_LENGTH + 1);
			writeIntLE(len, outputBytes, MAGIC_LENGTH + 5);
			writeIntLE(check, outputBytes, MAGIC_LENGTH + 9);

			outputBuf.tail(HEADER_LENGTH + compressedLength);

			return outputBuf;
		}

		@Override
		public ByteBuf encodeEndOfStreamBlock() {
			return ByteBuf.wrapForReading(MAGIC_AND_LAST_BYTES);
		}

		private static int compressionLevel(int blockSize) {
			int compressionLevel = 32 - Integer.numberOfLeadingZeros(blockSize - 1); // ceil of log2
			if (CHECKS) {
				checkArgument((1 << compressionLevel) >= blockSize);
				checkArgument(blockSize * 2 > (1 << compressionLevel));
			}
			compressionLevel = max(0, compressionLevel - COMPRESSION_LEVEL_BASE);
			if (CHECKS) {
				checkArgument(compressionLevel <= 0x0F);
			}
			return compressionLevel;
		}

		private static void writeIntLE(int i, byte[] buf, int off) {
			buf[off++] = (byte) i;
			buf[off++] = (byte) (i >>> 8);
			buf[off++] = (byte) (i >>> 16);
			buf[off] = (byte) (i >>> 24);
		}
	}

	@Deprecated
	public static final class Decoder implements BlockDecoder {
		private final LZ4FastDecompressor decompressor;
		private final Checksum checksum;
		private final boolean ignoreMissingEndOfStreamBlock;

		private int originalLen;
		private int compressedLen;
		private int compressionMethod;
		private int check;
		private boolean endOfStream;

		private boolean readingHeader = true;

		private final IntLeByteScanner intLEScanner = new IntLeByteScanner();

		Decoder(LZ4FastDecompressor decompressor, Checksum checksum, boolean ignoreMissingEndOfStreamBlock) {
			this.decompressor = decompressor;
			this.checksum = checksum;
			this.ignoreMissingEndOfStreamBlock = ignoreMissingEndOfStreamBlock;
		}

		@Override
		public void reset() {
			endOfStream = false;
		}

		@Override
		public @Nullable ByteBuf decode(ByteBufs bufs) throws MalformedDataException {
			if (readingHeader) {
				if (!readHeader(bufs)) return null;
				readingHeader = false;
			}

			if (!bufs.hasRemainingBytes(compressedLen)) return null;
			readingHeader = true;

			if (endOfStream) return END_OF_STREAM;
			return decompressBody(bufs);
		}

		@Override
		public boolean ignoreMissingEndOfStreamBlock() {
			return ignoreMissingEndOfStreamBlock;
		}

		private boolean readHeader(ByteBufs bufs) throws MalformedDataException {
			bufs.scanBytes((index, value) -> {
				if (value != MAGIC[index]) {
					throw new UnknownFormatException(
						"Expected stream to start with bytes: " + Arrays.toString(MAGIC));
				}
				return index == MAGIC_LENGTH - 1;
			});

			if (!bufs.hasRemainingBytes(HEADER_LENGTH)) return false;
			bufs.skip(MAGIC_LENGTH);

			int token = bufs.getByte() & 0xFF;
			compressionMethod = token & 0xF0;
			int compressionLevel = COMPRESSION_LEVEL_BASE + (token & 0x0F);
			if (compressionMethod != COMPRESSION_METHOD_RAW && compressionMethod != COMPRESSION_METHOD_LZ4) {
				throw new UnknownFormatException("Unknown compression method");
			}

			compressedLen = readInt(bufs);
			originalLen = readInt(bufs);
			check = readInt(bufs);
			if (originalLen > 1 << compressionLevel
				|| (originalLen < 0 || compressedLen < 0)
				|| (originalLen == 0 && compressedLen != 0)
				|| (originalLen != 0 && compressedLen == 0)
				|| (compressionMethod == COMPRESSION_METHOD_RAW && originalLen != compressedLen)
			) {
				throw new MalformedDataException("Malformed header");
			}
			if (originalLen == 0) {
				if (check != 0) {
					throw new MalformedDataException("Checksum in last block is not allowed");
				}
				endOfStream = true;
			}

			return true;
		}

		private int readInt(ByteBufs bufs) throws MalformedDataException {
			bufs.consumeBytes(intLEScanner);
			return intLEScanner.value;
		}

		private ByteBuf decompressBody(ByteBufs bufs) throws MalformedDataException {
			ByteBuf inputBuf = bufs.takeExactSize(compressedLen);
			ByteBuf outputBuf = ByteBufPool.allocate(originalLen);
			try {
				byte[] bytes = inputBuf.array();
				int off = inputBuf.head();
				outputBuf.tail(originalLen);
				if (compressionMethod == COMPRESSION_METHOD_RAW) {
					System.arraycopy(bytes, off, outputBuf.array(), 0, originalLen);
				} else {
					assert compressionMethod == COMPRESSION_METHOD_LZ4;
					try {
						int compressedLen = decompressor.decompress(bytes, off, outputBuf.array(), 0, originalLen);
						if (compressedLen != this.compressedLen) {
							throw new InvalidSizeException("Actual size of decompressed data does not equal expected size of decompressed data");
						}
					} catch (LZ4Exception e) {
						throw new MalformedDataException("Failed to decompress data", e);
					}
				}
				checksum.reset();
				checksum.update(outputBuf.array(), 0, originalLen);
				if (checksum.getValue() != check) {
					throw new MalformedDataException("Checksums do not match. Received: (" + check + "), actual: (" + checksum.getValue() + ')');
				}
				return outputBuf;
			} catch (Exception e) {
				outputBuf.recycle();
				throw e;
			} finally {
				inputBuf.recycle();
			}
		}

		public static final class IntLeByteScanner implements ByteBufs.ByteScanner {
			int value;

			@Override
			public boolean consume(int index, byte b) {
				value = (index == 0 ? 0 : value >>> 8) | (b & 0xFF) << 24;
				return index == 3;
			}
		}

	}
}
