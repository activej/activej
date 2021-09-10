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

import io.activej.http.CaseInsensitiveTokenMap.Token;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.activej.bytebuf.ByteBufStrings.decodeAscii;

/**
 * This is a specialized token to be used in {@link CaseInsensitiveTokenMap} for header names.
 */
public final class HttpHeader extends Token {

	HttpHeader(int hashCodeCI, byte[] bytes, int offset, int length, byte @Nullable [] lowerCaseBytes) {
		super(hashCodeCI, bytes, offset, length, lowerCaseBytes);
	}

	public int size() {
		return length;
	}

	public int writeTo(byte[] array, int offset) {
		System.arraycopy(bytes, this.offset, array, offset, length);
		return length + offset;
	}

	@Override
	public int hashCode() {
		return hashCodeCI;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof HttpHeader)) return false;
		HttpHeader that = (HttpHeader) o;

		if (length != that.length) return false;
		for (int i = 0; i < length; i++) {
			byte thisChar = this.bytes[offset + i];
			byte thatChar = that.bytes[that.offset + i];
			if (thisChar >= 'A' && thisChar <= 'Z')
				thisChar += 'a' - 'A';
			if (thatChar >= 'A' && thatChar <= 'Z')
				thatChar += 'a' - 'A';
			if (thisChar != thatChar)
				return false;
		}
		return true;
	}

	@Override
	public @NotNull String toString() {
		return decodeAscii(bytes, offset, length);
	}
}
