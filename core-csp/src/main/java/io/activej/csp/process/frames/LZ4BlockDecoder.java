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
import io.activej.common.exception.parse.ParseException;
import io.activej.common.exception.parse.UnknownFormatException;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static io.activej.csp.process.frames.LZ4FrameFormat.*;

final class LZ4BlockDecoder implements BlockDecoder {
	private static final ParseException STREAM_IS_CORRUPTED = new ParseException(LZ4BlockDecoder.class, "Stream is corrupted");
	private static final UnknownFormatException UNKNOWN_FORMAT_EXCEPTION = new UnknownFormatException(LZ4FrameFormat.class,
			"Expected stream to start with bytes: " + Arrays.toString(MAGIC));

	private static final int LAST_BLOCK_INT = 0xffffffff;

	private final LZ4FastDecompressor decompressor;

	private final byte[] headerBuf = new byte[MAGIC_LENGTH];
	private final byte[] intBuf = new byte[4];

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
		final ByteBuf firstBuf = bufs.peekBuf();
		if (firstBuf == null) return null;

		final byte[] bytes = firstBuf.array();
		final int tail = firstBuf.tail();
		int head = firstBuf.head();

		if (readHeader) {
			if (!readHeader(bufs, bytes, head, tail)) return null;
			readHeader = false;
			head += MAGIC_LENGTH;
		}

		if (!bufs.hasRemainingBytes(4)) return null;
		int compressedSize = peekInt(bufs, bytes, head, tail);

		if (compressedSize >= 0) {
			if (!bufs.hasRemainingBytes(4 + compressedSize + 1)) return null;
			bufs.skip(4);
			ByteBuf result = bufs.takeExactSize(compressedSize + 1);
			if (result.at(result.tail() - 1) != END_OF_BLOCK) {
				throw STREAM_IS_CORRUPTED;
			}
			result.moveTail(-1);
			return result;
		}

		return decompress(bufs, compressedSize, bytes, head + 4, tail);
	}

	@Override
	public boolean ignoreMissingEndOfStreamBlock() {
		return false;
	}

	private boolean readHeader(ByteBufQueue bufs, byte[] array, int head, int tail) throws UnknownFormatException {
		int limit;
		if (tail - head < MAGIC_LENGTH) {
			limit = bufs.peekTo(headerBuf, 0, MAGIC_LENGTH);
			array = headerBuf;
			head = 0;
		} else {
			limit = MAGIC_LENGTH;
		}

		for (int i = 0; i < limit; i++) {
			if (array[head + i] != MAGIC[i]) {
				throw UNKNOWN_FORMAT_EXCEPTION;
			}
		}

		if (limit != MAGIC_LENGTH) return false;

		bufs.skip(MAGIC_LENGTH);
		return true;
	}

	@Nullable
	private ByteBuf decompress(ByteBufQueue bufs, int compressedSize,
			final byte[] bytes, final int head, final int tail) throws ParseException {
		if (compressedSize == LAST_BLOCK_INT) {
			bufs.skip(4);
			return END_OF_STREAM;
		}   // data is compressed

		compressedSize = compressedSize & COMPRESSED_LENGTH_MASK;

		if (!bufs.hasRemainingBytes(2 * 4 + compressedSize + 1)) return null;
		bufs.skip(4);
		int originalSize = peekInt(bufs, bytes, head, tail);
		if (originalSize < 0) {
			throw STREAM_IS_CORRUPTED;
		}
		bufs.skip(4);

		ByteBuf firstBuf = bufs.peekBuf();
		assert firstBuf != null; // ensured above

		ByteBuf compressedBuf = firstBuf.readRemaining() >= compressedSize + 1 ? firstBuf : bufs.takeExactSize(compressedSize + 1);

		if (compressedBuf.at(compressedBuf.head() + compressedSize) != END_OF_BLOCK) {
			throw STREAM_IS_CORRUPTED;
		}

		ByteBuf buf = ByteBufPool.allocate(originalSize);
		try {
			int readBytes = decompressor.decompress(compressedBuf.array(), compressedBuf.head(), buf.array(), 0, originalSize);
			if (readBytes != compressedSize) {
				buf.recycle();
				throw STREAM_IS_CORRUPTED;
			}
			buf.tail(originalSize);
		} catch (LZ4Exception e) {
			buf.recycle();
			throw new ParseException(LZ4BlockDecoder.class, "Stream is corrupted", e);
		}

		if (compressedBuf != firstBuf) {
			compressedBuf.recycle();
		} else {
			bufs.skip(compressedSize + 1);
		}

		return buf;
	}

	private int peekInt(ByteBufQueue bufs, byte[] bytes, int head, int tail) {
		if (tail - head < 4) {
			bufs.peekTo(intBuf, 0, 4);
			bytes = intBuf;
			head = 0;
		}

		return (bytes[head] & 0xFF) << 24
				| ((bytes[head + 1] & 0xFF) << 16)
				| ((bytes[head + 2] & 0xFF) << 8)
				| (bytes[head + 3] & 0xFF);
	}

}
