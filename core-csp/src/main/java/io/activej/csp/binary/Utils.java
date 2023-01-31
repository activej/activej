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

package io.activej.csp.binary;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;

public class Utils {

	public static ByteBufsCodec<ByteBuf, ByteBuf> nullTerminated() {
		return ByteBufsCodec
				.ofDelimiter(
						ByteBufsDecoder.ofNullTerminatedBytes(),
						buf -> {
							ByteBuf buf1 = ByteBufPool.ensureWriteRemaining(buf, 1);
							buf1.put((byte) 0);
							return buf1;
						});
	}

	static ByteBufsDecoder<ByteBuf> decodeUntilTerminatorByte(byte terminator, int maxSize) {
		return bufs -> {
			int bytes = bufs.scanBytes((index, nextByte) -> {
				if (nextByte == terminator) {
					return true;
				}
				if (index == maxSize - 1) {
					throw new MalformedDataException("No terminator byte is found in " + maxSize + " bytes");
				}
				return false;
			});

			if (bytes == 0) return null;

			ByteBuf buf = bufs.takeExactSize(bytes);
			buf.moveTail(-1);
			return buf;
		};
	}

	public static class VarIntByteScanner implements ByteBufs.ByteScanner {
		int result;

		@Override
		public boolean consume(int index, byte b) throws MalformedDataException {
			result = (index == 0 ? 0 : result) | (b & 0x7F) << index * 7;
			if ((b & 0x80) == 0) {
				return true;
			}
			if (index == 4) {
				throw new InvalidSizeException("VarInt is too long for a 32-bit integer");
			}
			return false;
		}
	}

	public static class IntByteScanner implements ByteBufs.ByteScanner {
		private int value;

		@Override
		public boolean consume(int index, byte b) {
			value = value << 8 | b & 0xFF;
			return index == 3;
		}

		public int getValue() {
			return value;
		}
	}
}
