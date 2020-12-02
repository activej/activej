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

import java.util.function.Supplier;
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

	private static final ConcurrentStack<Inflater> decompressors = new ConcurrentStack<>();
	private static final ConcurrentStack<Deflater> compressors = new ConcurrentStack<>();

	public static ByteBuf fromGzip(ByteBuf src, int maxMessageSize) throws ParseException {
		if (CHECK) checkArgument(src.readRemaining() > 0);

		int expectedSize = readExpectedInputSize(src);
		check(expectedSize >= 0, src, () -> new InvalidSizeException("Incorrect uncompressed input size"));
		check(expectedSize <= maxMessageSize, src, () -> new InvalidSizeException("Decompressed data size exceeds max expected size"));
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
			throw new ParseException("Data format exception");
		}
		moveDecompressorToPool(decompressor);
		check(expectedSize == dst.readRemaining(), src, dst, () ->
				new InvalidSizeException("Decompressed data size is not equal to input size from GZIP trailer"));
		check(src.readRemaining() == GZIP_FOOTER_SIZE, src, dst, () -> new ParseException("Compressed data was not read fully"));

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
		check(buf.readRemaining() >= 8, buf, () -> new ParseException("Corrupted GZIP header"));
		int w = buf.tail();
		int r = buf.head();
		// read decompressed data size, represented by little-endian int
		buf.head(w - 4);
		int bigEndianPosition = buf.readInt();
		buf.head(r);
		return Integer.reverseBytes(bigEndianPosition);
	}

	private static void processHeader(ByteBuf buf) throws ParseException {
		check(buf.readRemaining() >= GZIP_HEADER_SIZE, buf, () -> new ParseException("Corrupted GZIP header"));

		check(buf.readByte() == GZIP_HEADER[0], buf, () -> new ParseException("Incorrect identification bytes. Not in GZIP format"));
		check(buf.readByte() == GZIP_HEADER[1], buf, () -> new ParseException("Incorrect identification bytes. Not in GZIP format"));
		check(buf.readByte() == GZIP_HEADER[2], buf, () -> new UnknownFormatException("Unsupported compression method. Deflate compression required"));

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
		check(totalUncompressedBytesCount < maxSize, dst, src, () ->
				new InvalidSizeException("Decompressed data size exceeds max expected size"));
		check(decompressor.finished(), dst, src, () ->
				new InvalidSizeException("Decompressed data size is not equal to input size from GZIP trailer"));
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
		check(buf.readRemaining() >= 2, buf, () -> new ParseException("Corrupted GZIP header"));
		short subFieldDataSize = buf.readShort();
		short reversedSubFieldDataSize = Short.reverseBytes(subFieldDataSize);
		check(buf.readRemaining() >= reversedSubFieldDataSize, buf, () -> new ParseException("Corrupted GZIP header"));
		buf.moveHead(reversedSubFieldDataSize);
	}

	private static void skipToTerminatorByte(ByteBuf buf) throws ParseException {
		while (buf.readRemaining() > 0) {
			if (buf.get() == 0) {
				return;
			}
		}
		throw new ParseException("Corrupted GZIP header");
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

	private static void check(boolean condition, ByteBuf buf1, ByteBuf buf2, Supplier<ParseException> exceptionSupplier) throws ParseException {
		if (!condition) {
			buf1.recycle();
			buf2.recycle();
			throw exceptionSupplier.get();
		}
	}

	private static void check(boolean condition, ByteBuf buf, Supplier<ParseException> exceptionSupplier) throws ParseException {
		if (!condition) {
			buf.recycle();
			throw exceptionSupplier.get();
		}
	}
}
