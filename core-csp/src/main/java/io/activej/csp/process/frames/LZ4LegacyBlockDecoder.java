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
import net.jpountz.xxhash.StreamingXXHash32;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static io.activej.csp.process.frames.LZ4LegacyFrameFormat.*;
import static java.lang.Math.min;

@Deprecated
abstract class LZ4LegacyBlockDecoder implements BlockDecoder {
	protected static final ParseException STREAM_IS_CORRUPTED = new ParseException(LZ4LegacyBlockDecoder.class, "Stream is corrupted");
	protected static final UnknownFormatException UNKNOWN_FORMAT_EXCEPTION = new UnknownFormatException(LZ4LegacyFrameFormat.class,
			"Expected stream to start with bytes: " + Arrays.toString(MAGIC));

	private final StreamingXXHash32 checksum;
	private final byte[] headerBuf = new byte[HEADER_LENGTH];
	private final boolean ignoreMissingEndOfStreamBlock;

	protected int originalLen;
	protected int compressedLen;
	protected int compressionMethod;
	protected int check;
	protected boolean endOfStream;

	private boolean shouldReadHeader = true;

	LZ4LegacyBlockDecoder(StreamingXXHash32 checksum, boolean ignoreMissingEndOfStreamBlock) {
		this.checksum = checksum;
		this.ignoreMissingEndOfStreamBlock = ignoreMissingEndOfStreamBlock;
	}

	@Override
	public final void reset() {
		endOfStream = false;
	}

	@Nullable
	@Override
	public final ByteBuf decode(ByteBufQueue bufs) throws ParseException {
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
	public final boolean ignoreMissingEndOfStreamBlock() {
		return ignoreMissingEndOfStreamBlock;
	}

	protected abstract void decompress(ByteBuf outputBuf, byte[] bytes, int off) throws ParseException;

	private boolean readHeader(ByteBufQueue bufs) throws ParseException {
		ByteBuf buf = bufs.peekBuf();
		if (buf == null) return false;

		byte[] array;
		int off, limit;
		if (buf.readRemaining() >= HEADER_LENGTH) {
			array = buf.array();
			off = buf.head();
			limit = HEADER_LENGTH;
		} else {
			limit = bufs.peekTo(headerBuf, 0, HEADER_LENGTH);
			array = headerBuf;
			off = 0;
		}

		int magicLimit = min(MAGIC_LENGTH, limit);
		for (int i = 0; i < magicLimit; i++) {
			if (array[off + i] != MAGIC[i]) {
				throw UNKNOWN_FORMAT_EXCEPTION;
			}
		}

		if (limit != HEADER_LENGTH) return false;

		int token = array[off + MAGIC_LENGTH] & 0xFF;
		compressionMethod = token & 0xF0;
		int compressionLevel = COMPRESSION_LEVEL_BASE + (token & 0x0F);
		if (compressionMethod != COMPRESSION_METHOD_RAW && compressionMethod != COMPRESSION_METHOD_LZ4) {
			throw STREAM_IS_CORRUPTED;
		}

		compressedLen = readIntLE(array, off + MAGIC_LENGTH + 1);
		originalLen = readIntLE(array, off + MAGIC_LENGTH + 5);
		check = readIntLE(array, off + MAGIC_LENGTH + 9);
		if (originalLen > 1 << compressionLevel
				|| (originalLen < 0 || compressedLen < 0)
				|| (originalLen == 0 && compressedLen != 0)
				|| (originalLen != 0 && compressedLen == 0)
				|| (compressionMethod == COMPRESSION_METHOD_RAW && originalLen != compressedLen)) {
			throw STREAM_IS_CORRUPTED;
		}
		if (originalLen == 0) {
			if (check != 0) {
				throw STREAM_IS_CORRUPTED;
			}
			endOfStream = true;
		}

		bufs.skip(HEADER_LENGTH);
		return true;
	}

	private static int readIntLE(byte[] buf, int offset) {
		return (buf[offset] & 0xFF) |
				((buf[offset + 1] & 0xFF) << 8) |
				((buf[offset + 2] & 0xFF) << 16) |
				((buf[offset + 3] & 0xFF) << 24);
	}

	private ByteBuf decompressBody(ByteBufQueue queue) throws ParseException {
		ByteBuf inputBuf = queue.takeExactSize(compressedLen);
		ByteBuf outputBuf = ByteBufPool.allocate(originalLen);
		try {
			byte[] bytes = inputBuf.array();
			int off = inputBuf.head();
			outputBuf.tail(originalLen);
			switch (compressionMethod) {
				case COMPRESSION_METHOD_RAW:
					System.arraycopy(bytes, off, outputBuf.array(), 0, originalLen);
					break;
				case COMPRESSION_METHOD_LZ4:
					try {
						decompress(outputBuf, bytes, off);
					} catch (LZ4Exception e) {
						throw new ParseException(LZ4LegacyBlockDecoder.class, "Stream is corrupted", e);
					}
					break;
				default:
					throw STREAM_IS_CORRUPTED;
			}
			checksum.reset();
			checksum.update(outputBuf.array(), 0, originalLen);
			if (checksum.getValue() != check) {
				throw STREAM_IS_CORRUPTED;
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
