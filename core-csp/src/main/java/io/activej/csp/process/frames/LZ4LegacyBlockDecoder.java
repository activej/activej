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

package io.activej.csp.process.frames;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.UnknownFormatException;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.zip.Checksum;

import static io.activej.csp.process.frames.LZ4LegacyFrameFormat.*;

@Deprecated
final class LZ4LegacyBlockDecoder implements BlockDecoder {
	private final LZ4FastDecompressor decompressor;
	private final Checksum checksum;
	private final boolean ignoreMissingEndOfStreamBlock;

	private int originalLen;
	private int compressedLen;
	private int compressionMethod;
	private int check;
	private boolean endOfStream;

	private boolean readingHeader = true;

	private final IntLeScanner intLEScanner = new IntLeScanner();

	LZ4LegacyBlockDecoder(LZ4FastDecompressor decompressor, Checksum checksum, boolean ignoreMissingEndOfStreamBlock) {
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
				|| (compressionMethod == COMPRESSION_METHOD_RAW && originalLen != compressedLen)) {
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

	private static final class IntLeScanner implements ByteBufs.ByteScanner {
		public int value;

		@Override
		public boolean consume(int index, byte b) {
			value = (index == 0 ? 0 : value >>> 8) | (b & 0xFF) << 24;
			return index == 3;
		}
	}


}
