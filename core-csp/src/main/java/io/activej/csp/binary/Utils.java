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
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;

class Utils {

	static ByteBufsDecoder<ByteBuf> parseUntilTerminatorByte(byte terminator, int maxSize) {
		return bufs -> {
			int result = bufs.scanBytes((index, nextByte) -> {
				if (nextByte == terminator) {
					return true;
				}
				if (index == maxSize - 1) {
					throw new MalformedDataException("No terminator byte is found in " + maxSize + " bytes");
				}
				return false;
			});

			if (result == -1) return null;

			ByteBuf buf = bufs.takeExactSize(result);
			bufs.skip(1);
			return buf;
		};
	}

	static class VarIntScanner implements ByteBufQueue.ByteScanner {
		private int result;

		@Override
		public boolean consume(int index, byte nextByte) throws MalformedDataException {
			result |= (nextByte & 0x7F) << index * 7;
			if ((nextByte & 0x80) == 0) {

				return true;
			}
			if (index == 4) {
				throw new InvalidSizeException("VarInt is too long for a 32-bit integer");
			}

			return false;
		}

		public int getResult() {
			return result;
		}
	}

	static class IntScanner implements ByteBufQueue.ByteScanner {
		private int result;

		@Override
		public boolean consume(int index, byte nextByte) {
			result <<= 8;
			result |= (nextByte & 0xFF);
			return index == 3;
		}

		public int getResult() {
			return result;
		}
	}
}
