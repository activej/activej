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

import static io.activej.common.Checks.checkArgument;
import static io.activej.csp.process.frames.LZ4FrameFormat.*;

public final class LZ4BlockEncoder implements BlockEncoder {
	private static final boolean CHECKS = Checks.isEnabled(LZ4BlockEncoder.class);

	private final LZ4Compressor compressor;
	private boolean writeHeader = true;

	LZ4BlockEncoder(LZ4Compressor compressor) {
		this.compressor = compressor;
	}

	@Override
	public void reset() {
		writeHeader = true;
	}

	@Override
	public ByteBuf encode(ByteBuf inputBuf) {
		int headerSize = writeHeader ? MAGIC_LENGTH : 0;
		writeHeader = false;

		int off = inputBuf.head();
		int len = inputBuf.readRemaining();
		byte[] array = inputBuf.array();

		if (CHECKS) checkArgument(len != 0, "Encoding empty buf");

		ByteBuf outputBuf = ByteBufPool.allocate(headerSize + 2 * 4 + compressor.maxCompressedLength(len) + 1);

		if (headerSize != 0) {
			System.arraycopy(MAGIC, 0, outputBuf.array(), 0, MAGIC_LENGTH);
			outputBuf.moveTail(MAGIC_LENGTH);
		}

		int compressedLength = compressor.compress(array, off, len, outputBuf.array(), headerSize + 2 * 4);

		if (compressedLength + 4 < len) {
			outputBuf.writeInt(compressedLength | ~COMPRESSED_LENGTH_MASK);
			outputBuf.writeInt(len);
			outputBuf.moveTail(compressedLength);
		} else {
			outputBuf.writeInt(len);
			System.arraycopy(array, off, outputBuf.array(), outputBuf.tail(), len);
			outputBuf.moveTail(len);
		}
		outputBuf.put(END_OF_BLOCK);

		return outputBuf;
	}

	@Override
	public ByteBuf encodeEndOfStreamBlock() {
		if (!writeHeader) return ByteBuf.wrapForReading(LAST_BLOCK_BYTES);
		writeHeader = false;
		return ByteBuf.wrapForReading(MAGIC_AND_LAST_BLOCK_BYTES);
	}

}
