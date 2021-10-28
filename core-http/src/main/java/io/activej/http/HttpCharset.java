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

import io.activej.common.ApplicationSettings;
import io.activej.http.CaseInsensitiveTokenMap.Token;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.activej.bytebuf.ByteBufStrings.decodeAscii;
import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.common.Utils.arraysEquals;
import static java.nio.charset.Charset.forName;

/**
 * This is a specialized token to be used in {@link CaseInsensitiveTokenMap} for charset header values.
 */
public final class HttpCharset extends Token {
	private static final int SLOTS_NUMBER = ApplicationSettings.getInt(HttpCharset.class, "slotsNumber", 512);
	private static final int MAX_PROBINGS = ApplicationSettings.getInt(HttpCharset.class, "maxProbings", 2);

	private static final CaseInsensitiveTokenMap<HttpCharset> charsets = new CaseInsensitiveTokenMap<>(SLOTS_NUMBER, MAX_PROBINGS, HttpCharset::new, HttpCharset[]::new);
	private static final Map<Charset, HttpCharset> java2http = new HashMap<>();

	public static final HttpCharset UTF_8 = register("utf-8", StandardCharsets.UTF_8);
	public static final HttpCharset US_ASCII = register("us-ascii", StandardCharsets.US_ASCII);
	public static final HttpCharset LATIN_1 = register("iso-8859-1", StandardCharsets.ISO_8859_1);

	private Charset javaCharset;

	// maximum of 40 characters, us-ascii, see rfc2978,
	// http://www.iana.org/assignments/character-sets/character-sets.txt
	private HttpCharset(int hashCodeCI, byte[] bytes, int offset, int length, byte @Nullable [] lowerCaseBytes) {
		super(hashCodeCI, bytes, offset, length, lowerCaseBytes);
	}

	public static HttpCharset register(String charsetName, Charset charset){
		return charsets.register(charsetName).addCharset(charset);
	}

	static HttpCharset of(Charset charset) {
		HttpCharset hCharset = java2http.get(charset);
		if (hCharset != null) {
			return hCharset;
		} else {
			byte[] bytes = encodeAscii(charset.name());
			return decode(bytes, 0, bytes.length);
		}
	}

	static HttpCharset decode(byte[] bytes, int pos, int length) {
		return charsets.getOrCreate(bytes, pos, length);
	}

	static int render(HttpCharset charset, byte[] container, int pos) {
		System.arraycopy(charset.bytes, charset.offset, container, pos, charset.length);
		return charset.length;
	}

	private HttpCharset addCharset(Charset charset) {
		javaCharset = charset;
		java2http.put(charset, this);
		return this;
	}

	Charset toJavaCharset() throws MalformedHttpException {
		if (javaCharset == null) {
			String charsetName = decodeAscii(bytes, offset, length);
			try {
				if (charsetName.startsWith("\"") || charsetName.startsWith("'")) {
					charsetName = charsetName.substring(1, charsetName.length() - 1);
				}
				javaCharset = forName(charsetName);
			} catch (Exception e) {
				throw new MalformedHttpException("Can't fetch charset for " + charsetName, e);
			}
		}
		return javaCharset;
	}

	int size() {
		return length;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		HttpCharset that = (HttpCharset) o;
		return arraysEquals(bytes, offset, length, that.bytes, that.offset, that.length);
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
