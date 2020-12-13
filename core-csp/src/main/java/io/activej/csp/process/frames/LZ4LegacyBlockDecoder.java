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
import net.jpountz.xxhash.StreamingXXHash32;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static io.activej.csp.process.frames.LZ4LegacyFrameFormat.*;

@Deprecated
final class LZ4LegacyBlockDecoder implements BlockDecoder {
	private static final ParseException STREAM_IS_CORRUPTED = new ParseException(LZ4LegacyBlockDecoder.class, "Stream is corrupted");
	private static final UnknownFormatException UNKNOWN_FORMAT_EXCEPTION = new UnknownFormatException(LZ4LegacyFrameFormat.class,
			"Expected stream to start with bytes: " + Arrays.toString(MAGIC));

	private final LZ4FastDecompressor decompressor;
	private final StreamingXXHash32 checksum;
	private final boolean ignoreMissingEndOfStreamBlock;

	private int originalLen;
	private int compressedLen;
	private int compressionMethod;
	private int check;
	private boolean endOfStream;

	private boolean readingHeader = true;

	private final IntLeScanner intLEScanner = new IntLeScanner();

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
	public ByteBuf decode(ByteBufQueue bufs) throws ParseException {
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

	private boolean readHeader(ByteBufQueue bufs) throws ParseException {
		bufs.scanBytes((index, value) -> {
			if (value != MAGIC[index]) {
				throw UNKNOWN_FORMAT_EXCEPTION;
			}
			return index == MAGIC_LENGTH - 1;
		});

		if (!bufs.hasRemainingBytes(HEADER_LENGTH)) return false;
		bufs.skip(MAGIC_LENGTH);

		int token = bufs.getByte() & 0xFF;
		compressionMethod = token & 0xF0;
		int compressionLevel = COMPRESSION_LEVEL_BASE + (token & 0x0F);
		if (compressionMethod != COMPRESSION_METHOD_RAW && compressionMethod != COMPRESSION_METHOD_LZ4) {
			throw STREAM_IS_CORRUPTED;
		}

		compressedLen = readInt(bufs);
		originalLen = readInt(bufs);
		check = readInt(bufs);
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

		return true;
	}

	private int readInt(ByteBufQueue bufs) throws ParseException {
		bufs.consumeBytes(intLEScanner);
		return intLEScanner.value;
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
						int compressedLen = decompressor.decompress(bytes, off, outputBuf.array(), 0, originalLen);
						if (compressedLen != this.compressedLen) {
							throw STREAM_IS_CORRUPTED;
						}
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

	private static final class IntLeScanner implements ByteBufQueue.ByteScanner {
		public int value;

		@Override
		public boolean consume(int index, byte b) throws ParseException {
			value = (index == 0 ? 0 : value >>> 8) | (b & 0xFF) << 24;
			return index == 3;
		}
	}


}
