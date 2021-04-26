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
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static io.activej.bytebuf.ByteBufStrings.decodeAscii;
import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.common.Utils.arraysEquals;
import static io.activej.http.HttpUtils.hashCodeCI;

/**
 * This is a specialized token to be used in {@link CaseInsensitiveTokenMap} for media type header values.
 */
public final class MediaType extends Token {
	// All media type values, subtype values, and parameter names as defined are case-insensitive RFC2045 section 2
	MediaType(int hashCodeCI, byte[] bytes, int offset, int length, byte @Nullable [] lowerCaseBytes) {
		super(hashCodeCI, bytes, offset, length, lowerCaseBytes);
	}

	public static MediaType of(String mime) {
		byte[] bytes = encodeAscii(mime);
		return MediaTypes.of(hashCodeCI(bytes), bytes, 0, bytes.length);
	}

	int size() {
		return bytes.length;
	}

	public boolean isTextType() {
		return bytes.length > 5
				&& bytes[0] == 't'
				&& bytes[1] == 'e'
				&& bytes[2] == 'x'
				&& bytes[3] == 't'
				&& bytes[4] == '/';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		MediaType that = (MediaType) o;
		return arraysEquals(this.bytes, this.offset, this.length, that.bytes, that.offset, that.length);
	}

	@Override
	public int hashCode() {
		int result = Arrays.hashCode(bytes);
		result = 31 * result + offset;
		result = 31 * result + length;
		return result;
	}

	@Override
	public String toString() {
		return decodeAscii(bytes, offset, length);
	}
}
