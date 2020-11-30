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

package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.Checks;
import io.activej.common.collection.ConcurrentStack;
import io.activej.common.exception.parse.InvalidSizeException;
import io.activej.common.exception.parse.ParseException;
import io.activej.common.exception.parse.UnknownFormatException;

import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static io.activej.common.Checks.checkArgument;

/**
 * This class contains various utils for the DEFLATE algorithm.
 */
public final class GzipProcessorUtils {
	private static final boolean CHECK = Checks.isEnabled(GzipProcessorUtils.class);

	// rfc 1952 section 2.3.1
	private static final byte[] GZIP_HEADER = {(byte) 0x1f, (byte) 0x8b, Deflater.DEFLATED, 0, 0, 0, 0, 0, 0, 0};
	private static final int GZIP_HEADER_SIZE = GZIP_HEADER.length;
	private static final int GZIP_FOOTER_SIZE = 8;

	private static final int FHCRC = 2;
	private static final int FEXTRA = 4;
	private static final int FNAME = 8;
	private static final int FCOMMENT = 16;

	private static final int SPARE_BYTES_COUNT = 8;

	// https://stackoverflow.com/a/23578269
	private static final double DEFLATE_MAX_BYTES_OVERHEAD_PER_16K_BLOCK = 5;

	private static final ParseException CORRUPTED_GZIP_HEADER = new ParseException(GzipProcessorUtils.class, "Corrupted GZIP header");
	private static final ParseException DECOMPRESSED_SIZE_EXCEEDS_EXPECTED_MAX_SIZE = new InvalidSizeException(GzipProcessorUtils.class, "Decompressed data size exceeds max expected size");
	private static final ParseException COMPRESSED_DATA_WAS_NOT_READ_FULLY = new ParseException(GzipProcessorUtils.class, "Compressed data was not read fully");
	private static final ParseException DATA_FORMAT_EXCEPTION = new ParseException(GzipProcessorUtils.class, "Data format exception");
	private static final ParseException ACTUAL_DECOMPRESSED_DATA_SIZE_IS_NOT_EQUAL_TO_EXPECTED = new InvalidSizeException(GzipProcessorUtils.class, "Decompressed data size is not equal to input size from GZIP trailer");
	private static final ParseException INCORRECT_ID_HEADER_BYTES = new ParseException(GzipProcessorUtils.class, "Incorrect identification bytes. Not in GZIP format");
	private static final ParseException INCORRECT_UNCOMPRESSED_INPUT_SIZE = new InvalidSizeException(GzipProcessorUtils.class, "Incorrect uncompressed input size");
	private static final ParseException UNSUPPORTED_COMPRESSION_METHOD = new UnknownFormatException(GzipProcessorUtils.class, "Unsupported compression method. Deflate compression required");

	private static final ConcurrentStack<Inflater> decompressors = new ConcurrentStack<>();
	private static final ConcurrentStack<Deflater> compressors = new ConcurrentStack<>();

	public static ByteBuf fromGzip(ByteBuf src, int maxMessageSize) throws ParseException {
		if (CHECK) checkArgument(src.readRemaining() > 0);

		int expectedSize = readExpectedInputSize(src);
		check(expectedSize >= 0, src, INCORRECT_UNCOMPRESSED_INPUT_SIZE);
		check(expectedSize <= maxMessageSize, src, DECOMPRESSED_SIZE_EXCEEDS_EXPECTED_MAX_SIZE);
		processHeader(src);
		ByteBuf dst = ByteBufPool.allocate(expectedSize);
		Inflater decompressor = ensureDecompressor();
		decompressor.setInput(src.array(), src.head(), src.readRemaining());
		try {
			readDecompressedData(decompressor, src, dst, maxMessageSize);
		} catch (DataFormatException ignored) {
			moveDecompressorToPool(decompressor);
			src.recycle();
			dst.recycle();
			throw DATA_FORMAT_EXCEPTION;
		}
		moveDecompressorToPool(decompressor);
		check(expectedSize == dst.readRemaining(), src, dst, ACTUAL_DECOMPRESSED_DATA_SIZE_IS_NOT_EQUAL_TO_EXPECTED);
		check(src.readRemaining() == GZIP_FOOTER_SIZE, src, dst, COMPRESSED_DATA_WAS_NOT_READ_FULLY);

		src.recycle();
		return dst;
	}

	public static ByteBuf toGzip(ByteBuf src) {
		if (CHECK) checkArgument(src.readRemaining() >= 0);

		Deflater compressor = ensureCompressor();
		compressor.setInput(src.array(), src.head(), src.readRemaining());
		compressor.finish();
		int dataSize = src.readRemaining();
		int crc = getCrc(src, dataSize);
		int maxDataSize = estimateMaxCompressedSize(dataSize);
		ByteBuf dst = ByteBufPool.allocate(GZIP_HEADER_SIZE + maxDataSize + GZIP_FOOTER_SIZE + SPARE_BYTES_COUNT);
		dst.put(GZIP_HEADER);
		dst = writeCompressedData(compressor, src, dst);
		dst.writeInt(Integer.reverseBytes(crc));
		dst.writeInt(Integer.reverseBytes(dataSize));

		moveCompressorToPool(compressor);
		src.recycle();
		return dst;
	}

	private static int readExpectedInputSize(ByteBuf buf) throws ParseException {
		// trailer size - 8 bytes. 4 bytes for CRC32, 4 bytes for ISIZE
		check(buf.readRemaining() >= 8, buf, CORRUPTED_GZIP_HEADER);
		int w = buf.tail();
		int r = buf.head();
		// read decompressed data size, represented by little-endian int
		buf.head(w - 4);
		int bigEndianPosition = buf.readInt();
		buf.head(r);
		return Integer.reverseBytes(bigEndianPosition);
	}

	private static void processHeader(ByteBuf buf) throws ParseException {
		check(buf.readRemaining() >= GZIP_HEADER_SIZE, buf, CORRUPTED_GZIP_HEADER);

		check(buf.readByte() == GZIP_HEADER[0], buf, INCORRECT_ID_HEADER_BYTES);
		check(buf.readByte() == GZIP_HEADER[1], buf, INCORRECT_ID_HEADER_BYTES);
		check(buf.readByte() == GZIP_HEADER[2], buf, UNSUPPORTED_COMPRESSION_METHOD);

		// skip optional fields
		byte flag = buf.readByte();
		buf.moveHead(6);
		if ((flag & FEXTRA) > 0) {
			skipExtra(buf);
		}
		if ((flag & FNAME) > 0) {
			skipToTerminatorByte(buf);
		}
		if ((flag & FCOMMENT) > 0) {
			skipToTerminatorByte(buf);
		}
		if ((flag & FHCRC) > 0) {
			buf.moveHead(2);
		}
	}

	private static void readDecompressedData(Inflater decompressor, ByteBuf src, ByteBuf dst, int maxSize) throws DataFormatException, ParseException {
		int totalUncompressedBytesCount = 0;
		int count = decompressor.inflate(dst.array(), dst.tail(), dst.writeRemaining());
		totalUncompressedBytesCount += count;
		dst.moveTail(count);
		check(totalUncompressedBytesCount < maxSize, dst, src, DECOMPRESSED_SIZE_EXCEEDS_EXPECTED_MAX_SIZE);
		check(decompressor.finished(), dst, src, ACTUAL_DECOMPRESSED_DATA_SIZE_IS_NOT_EQUAL_TO_EXPECTED);
		int totalRead = decompressor.getTotalIn();
		src.moveHead(totalRead);
	}

	private static int estimateMaxCompressedSize(int dataSize) {
		return (int) (dataSize + (dataSize / 16383 + 1) * DEFLATE_MAX_BYTES_OVERHEAD_PER_16K_BLOCK);
	}

	private static ByteBuf writeCompressedData(Deflater compressor, ByteBuf src, ByteBuf dst) {
		int unprocessedDataSize = src.readRemaining();
		while (!compressor.finished()) {
			int count = compressor.deflate(dst.array(), dst.tail(), dst.writeRemaining());
			dst.moveTail(count);
			if (compressor.finished()) {
				break;
			}
			int processedDataSize = compressor.getTotalIn();
			int newTailRemaining = estimateMaxCompressedSize(unprocessedDataSize - processedDataSize);
			dst = ByteBufPool.ensureWriteRemaining(dst, newTailRemaining);
		}
		src.moveHead(compressor.getTotalIn());
		return dst;
	}

	private static int getCrc(ByteBuf buf, int dataSize) {
		CRC32 crc32 = new CRC32();
		crc32.update(buf.array(), buf.head(), dataSize);
		return (int) crc32.getValue();
	}

	private static void skipExtra(ByteBuf buf) throws ParseException {
		check(buf.readRemaining() >= 2, buf, CORRUPTED_GZIP_HEADER);
		short subFieldDataSize = buf.readShort();
		short reversedSubFieldDataSize = Short.reverseBytes(subFieldDataSize);
		check(buf.readRemaining() >= reversedSubFieldDataSize, buf, CORRUPTED_GZIP_HEADER);
		buf.moveHead(reversedSubFieldDataSize);
	}

	private static void skipToTerminatorByte(ByteBuf buf) throws ParseException {
		while (buf.readRemaining() > 0) {
			if (buf.get() == 0) {
				return;
			}
		}
		throw CORRUPTED_GZIP_HEADER;
	}

	private static Inflater ensureDecompressor() {
		Inflater decompressor = decompressors.pop();
		if (decompressor == null) {
			decompressor = new Inflater(true);
		}
		return decompressor;
	}

	private static void moveDecompressorToPool(Inflater decompressor) {
		decompressor.reset();
		decompressors.push(decompressor);
	}

	private static Deflater ensureCompressor() {
		Deflater compressor = compressors.pop();
		if (compressor == null) {
			compressor = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
		}
		return compressor;
	}

	private static void moveCompressorToPool(Deflater compressor) {
		compressor.reset();
		compressors.push(compressor);
	}

	private static void check(boolean condition, ByteBuf buf1, ByteBuf buf2, ParseException e) throws ParseException {
		if (!condition) {
			buf1.recycle();
			buf2.recycle();
			throw e;
		}
	}

	private static void check(boolean condition, ByteBuf buf, ParseException e) throws ParseException {
		if (!condition) {
			buf.recycle();
			throw e;
		}
	}
}
