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
import io.activej.common.Checks;
import net.jpountz.lz4.LZ4Compressor;

import java.util.zip.Checksum;

import static io.activej.common.Checks.checkArgument;
import static io.activej.csp.process.frames.LZ4LegacyFrameFormat.*;
import static java.lang.Math.max;
import static java.lang.Math.min;

@Deprecated
public final class LZ4LegacyBlockEncoder implements BlockEncoder {
	private static final boolean CHECKS = Checks.isEnabled(LZ4LegacyBlockEncoder.class);

	private static final int MIN_BLOCK_SIZE = 64;
	private static final int MAX_BLOCK_SIZE = 1 << (COMPRESSION_LEVEL_BASE + 0x0F);

	private final LZ4Compressor compressor;
	private final Checksum checksum;

	LZ4LegacyBlockEncoder(LZ4Compressor compressor, Checksum checksum) {
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
