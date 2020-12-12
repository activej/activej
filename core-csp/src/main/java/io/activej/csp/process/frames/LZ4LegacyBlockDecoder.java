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
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.UnknownFormatException;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.xxhash.StreamingXXHash32;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static io.activej.csp.process.frames.LZ4LegacyFrameFormat.*;

@Deprecated
final class LZ4LegacyBlockDecoder implements BlockDecoder {
	private final LZ4FastDecompressor decompressor;
	private final StreamingXXHash32 checksum;
	private final boolean ignoreMissingEndOfStreamBlock;

	private int originalLen;
	private int compressedLen;
	private int compressionMethod;
	private int check;
	private boolean endOfStream;

	private boolean shouldReadHeader = true;

	LZ4LegacyBlockDecoder(LZ4FastDecompressor decompressor, StreamingXXHash32 checksum, boolean ignoreMissingEndOfStreamBlock) {
		this.decompressor = decompressor;
		this.checksum = checksum;
		this.ignoreMissingEndOfStreamBlock = ignoreMissingEndOfStreamBlock;
	}

	@Override
	public void reset() {
		endOfStream = false;
	}

	@Nullable
	@Override
	public ByteBuf decode(ByteBufQueue bufs) throws MalformedDataException {
		if (shouldReadHeader) {
			if (!readHeader(bufs)) return null;
			shouldReadHeader = false;
		}

		if (!bufs.hasRemainingBytes(compressedLen)) return null;
		shouldReadHeader = true;

		if (endOfStream) return END_OF_STREAM;
		return decompressBody(bufs);
	}

	@Override
	public boolean ignoreMissingEndOfStreamBlock() {
		return ignoreMissingEndOfStreamBlock;
	}

	private boolean readHeader(ByteBufQueue bufs) throws MalformedDataException {
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

		compressedLen = readIntLE(bufs);
		originalLen = readIntLE(bufs);
		check = readIntLE(bufs);
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

	/*
	 Using 'check' as an accumulator as it will be set last.
	 4 bytes in queue are asserted before call, hence warning suppression
	*/
	@SuppressWarnings("ConstantConditions")
	private int readIntLE(ByteBufQueue bufs) throws MalformedDataException {
		check = 0;
		return bufs.decodeBytes((index, nextByte) -> {
			check |= (nextByte & 0xFF) << 8 * index;
			return index == 3 ? check : null;
		});
	}

	private ByteBuf decompressBody(ByteBufQueue queue) throws MalformedDataException {
		ByteBuf inputBuf = queue.takeExactSize(compressedLen);
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

}
