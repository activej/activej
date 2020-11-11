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
import io.activej.common.exception.parse.InvalidSizeException;
import io.activej.common.exception.parse.ParseException;

import static io.activej.csp.binary.ByteBufsDecoder.NEGATIVE_SIZE;
import static io.activej.csp.binary.ByteBufsDecoder.SIZE_EXCEEDS_MAX_SIZE;

class Utils {

	static ByteBufsDecoder<ByteBuf> parseUntilTerminatorByte(byte terminator, int maxSize) {
		return bufs -> {
			int result = bufs.scanBytes((index, nextByte) -> {
				if (nextByte == terminator) {
					return true;
				}
				if (index == maxSize - 1) {
					throw new ParseException(ByteBufsDecoder.class, "No terminator byte is found in " + maxSize + " bytes");
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
		private final int maxSize;

		private int result;

		VarIntScanner(int maxSize) {
			this.maxSize = maxSize;
		}

		@Override
		public boolean consume(int index, byte nextByte) throws ParseException {
			result |= (nextByte & 0x7F) << index * 7;
			if ((nextByte & 0x80) == 0) {
				if (result < 0) throw NEGATIVE_SIZE;
				if (result > maxSize) throw SIZE_EXCEEDS_MAX_SIZE;
				return true;
			}
			if (index == 4) {
				throw new ParseException(ByteBufsDecoder.class, "VarInt is too long for 32-bit integer");
			}

			return false;
		}

		public int getResult() {
			return result;
		}
	}

	static class IntScanner implements ByteBufQueue.ByteScanner {
		private final int maxSize;

		private int result;

		IntScanner(int maxSize) {
			this.maxSize = maxSize;
		}

		@Override
		public boolean consume(int index, byte nextByte) throws ParseException {
			result <<= 8;
			result |= (nextByte & 0xFF);
			if (index == 3) {
				if (result < 0 || result > maxSize) {
					throw new InvalidSizeException(IntScanner.class,
							"Size is either less than 0 or greater than maxSize. Parsed size: " + result);
				}
				return true;
			}
			return false;
		}

		public int getResult() {
			return result;
		}
	}
}
