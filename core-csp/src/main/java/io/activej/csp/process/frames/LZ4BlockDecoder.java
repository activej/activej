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
import io.activej.common.exception.parse.InvalidSizeException;
import io.activej.common.exception.parse.ParseException;
import io.activej.common.exception.parse.UnknownFormatException;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static io.activej.csp.process.frames.LZ4FrameFormat.*;

final class LZ4BlockDecoder implements BlockDecoder {
	private static final int LAST_BLOCK_INT = 0xffffffff;

	private final LZ4FastDecompressor decompressor;

	private @Nullable Integer compressedSize;
	private int tempInt;

	private boolean readHeader = true;

	LZ4BlockDecoder(LZ4FastDecompressor decompressor) {
		this.decompressor = decompressor;
	}

	@Override
	public void reset() {
		readHeader = true;
	}

	@Nullable
	@Override
	public ByteBuf decode(ByteBufQueue bufs) throws ParseException {
		if (readHeader) {
			if (!readHeader(bufs)) return null;
			readHeader = false;
		}

		if (compressedSize == null && (compressedSize = readInt(bufs)) == null) return null;
		if (compressedSize == LAST_BLOCK_INT) return END_OF_STREAM;

		if (compressedSize >= 0) {
			if (!bufs.hasRemainingBytes(compressedSize + 1)) return null;
			ByteBuf result = bufs.takeExactSize(compressedSize + 1);
			if (result.at(result.tail() - 1) != END_OF_BLOCK) {
				throw new ParseException("Block does not end with special byte '1'");
			}
			result.moveTail(-1);
			compressedSize = null;
			return result;
		}
		return decompress(bufs);
	}

	@Override
	public boolean ignoreMissingEndOfStreamBlock() {
		return false;
	}

	private boolean readHeader(ByteBufQueue bufs) throws ParseException {
		return bufs.parseBytes((index, value) -> {
			if (value != MAGIC[index]) throw new UnknownFormatException("Expected stream to start with bytes: " + Arrays.toString(MAGIC));
			return index == MAGIC_LENGTH - 1 ? MAGIC : null;
		}) != null;
	}

	@Nullable
	private ByteBuf decompress(ByteBufQueue bufs) throws ParseException {
		assert compressedSize != null;

		int actualCompressedSize = compressedSize & COMPRESSED_LENGTH_MASK;

		if (!bufs.hasRemainingBytes(4 + actualCompressedSize + 1)) return null;
		//noinspection ConstantConditions - cannot be null as 4 bytes in queue are asserted above
		int originalSize = readInt(bufs);
		if (originalSize < 0 || originalSize > MAX_BLOCK_SIZE.toInt()) {
			throw new InvalidSizeException("Size (" + originalSize +
					") of block is either negative or exceeds max block size (" + MAX_BLOCK_SIZE + ')');
		}

		ByteBuf firstBuf = bufs.peekBuf();
		assert firstBuf != null; // ensured above

		ByteBuf compressedBuf = firstBuf.readRemaining() >= actualCompressedSize + 1 ? firstBuf : bufs.takeExactSize(actualCompressedSize + 1);

		if (compressedBuf.at(compressedBuf.head() + actualCompressedSize) != END_OF_BLOCK) {
			throw new ParseException("Block does not end with special byte '1'");
		}

		ByteBuf buf = ByteBufPool.allocate(originalSize);
		try {
			int readBytes = decompressor.decompress(compressedBuf.array(), compressedBuf.head(), buf.array(), 0, originalSize);
			if (readBytes != actualCompressedSize) {
				buf.recycle();
				throw new InvalidSizeException("Actual size of decompressed data does not equal expected size of decompressed data");
			}
			buf.tail(originalSize);
		} catch (LZ4Exception e) {
			buf.recycle();
			throw new ParseException("Failed to decompress data", e);
		}

		if (compressedBuf != firstBuf) {
			compressedBuf.recycle();
		} else {
			bufs.skip(actualCompressedSize + 1);
		}

		compressedSize = null;
		return buf;
	}

	@Nullable
	private Integer readInt(ByteBufQueue bufs) throws ParseException {
		return bufs.parseBytes((index, nextByte) -> {
			tempInt <<= 8;
			tempInt |= (nextByte & 0xFF);
			return index == 3 ? tempInt : null;
		});
	}
}
