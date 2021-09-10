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

import static io.activej.csp.process.frames.LZ4FrameFormat.*;

final class LZ4BlockDecoder implements BlockDecoder {
	private static final int LAST_BLOCK_INT = 0xffffffff;

	private final LZ4FastDecompressor decompressor;
	private boolean readHeader = true;

	private final IntScanner intScanner = new IntScanner();

	LZ4BlockDecoder(LZ4FastDecompressor decompressor) {
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
		int compressedSize = intScanner.value;
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
			if (value != MAGIC[index]) throw new UnknownFormatException("Expected stream to start with bytes: " + Arrays.toString(MAGIC));
			return index == MAGIC_LENGTH - 1;
		}) != 0;
	}

	private @Nullable ByteBuf decompress(ByteBufs bufs, int compressedSize) throws MalformedDataException {
		if (!bufs.hasRemainingBytes(4 + 4 + compressedSize + 1)) return null;

		bufs.consumeBytes(4, intScanner);
		int originalSize = intScanner.value;
		if (originalSize < 0 || originalSize > MAX_BLOCK_SIZE.toInt()) {
			throw new InvalidSizeException("Size (" + originalSize +
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

	private static final class IntScanner implements ByteBufs.ByteScanner {
		public int value;

		@Override
		public boolean consume(int index, byte b) {
			value = value << 8 | b & 0xFF;
			return index == 3;
		}
	}
}
