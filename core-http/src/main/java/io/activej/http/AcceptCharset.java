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

import java.nio.charset.Charset;
import java.util.List;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.common.Checks.checkArgument;
import static io.activej.http.HttpUtils.parseQ;
import static io.activej.http.HttpUtils.skipSpaces;

/**
 * This is a value class for the Accept-Charset header value.
 */
public final class AcceptCharset {
	public static final int DEFAULT_Q = 100;
	private static final byte[] Q_KEY = encodeAscii("q");

	private final HttpCharset charset;
	private final int q;

	private AcceptCharset(HttpCharset charset, int q) {
		this.charset = charset;
		this.q = q;
	}

	private AcceptCharset(HttpCharset charset) {
		this(charset, DEFAULT_Q);
	}

	public static AcceptCharset of(Charset charset) {
		return new AcceptCharset(HttpCharset.of(charset));
	}

	public static AcceptCharset of(Charset charset, int q) {
		checkArgument(q >= 0 && q <= 100, "Cannot create AcceptCharset with 'q' that is outside of bounds [0, 100]");
		return new AcceptCharset(HttpCharset.of(charset), q);
	}

	private static AcceptCharset of(HttpCharset charset) {
		return new AcceptCharset(charset);
	}

	private static AcceptCharset of(HttpCharset charset, int q) {
		return new AcceptCharset(charset, q);
	}

	public Charset getCharset() throws MalformedHttpException {
		return charset.toJavaCharset();
	}

	public int getQ() {
		return q;
	}

	static void parse(byte[] bytes, int pos, int len, List<AcceptCharset> list) throws MalformedHttpException {
		try {
			int end = pos + len;

			while (pos < end) {
				// parsing charset
				pos = skipSpaces(bytes, pos, end);
				int start = pos;
				while (pos < end && !(bytes[pos] == ';' || bytes[pos] == ',')) {
					pos++;
				}
				HttpCharset charset = HttpCharset.parse(bytes, start, pos - start);

				if (pos < end) {
					if (bytes[pos++] == ',') {
						list.add(AcceptCharset.of(charset));
					} else {
						int q = DEFAULT_Q;
						pos = skipSpaces(bytes, pos, end);
						start = pos;
						while (pos < end && bytes[pos] != ',') {
							if (bytes[pos] == '=' && equalsLowerCaseAscii(Q_KEY, bytes, start, pos - start)) {
								start = ++pos;
								while (pos < end && !(bytes[pos] == ';' || bytes[pos] == ',')) {
									pos++;
								}
								q = parseQ(bytes, start, pos - start);
								pos--;
							} else if (bytes[pos] == ';') {
								pos = skipSpaces(bytes, pos + 1, end);
								start = pos;
							}
							pos++;
						}
						list.add(AcceptCharset.of(charset, q));
						pos++;
					}
				} else {
					list.add(AcceptCharset.of(charset));
				}
			}
		} catch (RuntimeException e) {
			throw new MalformedHttpException("Failed to decode accept-charset", e);
		}
	}

	static void render(List<AcceptCharset> charsets, ByteBuf buf) {
		int pos = render(charsets, buf.array(), buf.tail());
		buf.tail(pos);
	}

	static int render(List<AcceptCharset> charsets, byte[] bytes, int pos) {
		for (int i = 0; i < charsets.size(); i++) {
			AcceptCharset charset = charsets.get(i);
			pos += HttpCharset.render(charset.charset, bytes, pos);
			if (charset.q != DEFAULT_Q) {
				bytes[pos++] = ';';
				bytes[pos++] = ' ';
				bytes[pos++] = 'q';
				bytes[pos++] = '=';
				bytes[pos++] = '0';
				bytes[pos++] = '.';
				int q = charset.q;
				if (q % 10 == 0) q /= 10;
				pos += encodePositiveInt(bytes, pos, q);
			}
			if (i < charsets.size() - 1) {
				bytes[pos++] = ',';
				bytes[pos++] = ' ';
			}
		}
		return pos;
	}

	int estimateSize() {
		return charset.size() + 10;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		AcceptCharset that = (AcceptCharset) o;

		if (q != that.q) return false;
		return charset.equals(that.charset);
	}

	@Override
	public int hashCode() {
		int result = charset.hashCode();
		result = 31 * result + q;
		return result;
	}

	@Override
	public String toString() {
		return "AcceptCharset{charset=" + charset + ", q=" + q + '}';
	}
}
