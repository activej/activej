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
import io.activej.common.exception.parse.ParseException;
import io.activej.common.ref.RefInt;

class Utils {

	static final int FOUND = -1;

	static ByteBufsDecoder<ByteBuf> parseUntilTerminatorByte(byte terminator, int maxSize) {
		return bufs -> {
			RefInt index = new RefInt(0);
			int result = bufs.scanBytes(nextByte -> {
				if (nextByte == terminator) {
					index.set(FOUND);
					return true;
				}
				return index.inc() >= maxSize;
			});

			if (result == -1) return null;
			if (index.get() != FOUND) {
				throw new ParseException(ByteBufsDecoder.class, "No terminator byte is found in " + maxSize + " bytes");
			}

			ByteBuf buf = bufs.takeExactSize(result);
			bufs.skip(1);
			return buf;
		};
	}
}
