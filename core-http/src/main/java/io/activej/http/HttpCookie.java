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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.http.HttpUtils.*;

/**
 * This class represents an abstraction for HTTP Cookie with fast parsing algorithms.
 */
public final class HttpCookie {
	private abstract static class AvHandler {
		protected abstract void handle(HttpCookie cookie, byte[] bytes, int start, int end) throws MalformedHttpException;
	}

	private static final byte[] EXPIRES = encodeAscii("Expires");
	private static final int EXPIRES_HC = 433574931;
	private static final byte[] MAX_AGE = encodeAscii("Max-Age");
	private static final int MAX_AGE_HC = -1709216267;
	private static final byte[] DOMAIN = encodeAscii("Domain");
	private static final int DOMAIN_HC = -438693883;
	private static final byte[] PATH = encodeAscii("Path");
	private static final int PATH_HC = 4357030;
	private static final byte[] HTTPONLY = encodeAscii("HttpOnly");
	private static final int SECURE_HC = -18770248;
	private static final byte[] SECURE = encodeAscii("Secure");
	private static final int HTTP_ONLY_HC = -1939729611;

	// RFC 6265
	//
	// set-cookie-header  = "Set-Cookie:" SP set-cookie-string
	// set-cookie-string  = cookie-pair *( ";" SP cookie-av )
	// cookie-header      = "Cookie:" OWS cookie-string OWS
	// cookie-string      = cookie-pair *( ";" SP cookie-pair )
	//
	// cookie-pair        = cookie-name "=" cookie-value
	// cookie-name        = 1*<any CHAR except CTLs or separators>
	// cookie-value       = *cookie-octet / ( DQUOTE *cookie-octet DQUOTE )
	//
	// cookie-octet       = any CHAR except [",;\SPACE] and CTLs
	// separators         = "(" | ")" | "<" | ">" | "@"
	//                    | "," | ";" | ":" | "\" | <">
	//                    | "/" | "[" | "]" | "?" | "="
	//                    | "{" | "}" | SP  | HT
	// CTLs               = [\u0000...\0032, DEL]

	private final String name;
	private String value;
	private long expirationDate = -1;
	private int maxAge = -1;
	private String domain;
	private String path = "";
	private boolean secure;
	private boolean httpOnly;
	private String extension;

	// region builders
	private HttpCookie(String name, String value) {
		this.name = name;
		this.value = value;
	}

	public HttpCookie(String name, String value, String path) {
		this(name, value);
		this.path = path;
	}

	public static HttpCookie of(String name, String value, String path) {
		return new HttpCookie(name, value, path);
	}

	public static HttpCookie of(String name, String value) {
		return new HttpCookie(name, value);
	}

	public static HttpCookie of(String name) {
		return new HttpCookie(name, null);
	}

	public HttpCookie withValue(String value) {
		setValue(value);
		return this;
	}

	public HttpCookie withExpirationDate(Instant expirationDate) {
		// <rfc1123-date, defined in [RFC2616], Section 3.3.1>
		setExpirationDate(expirationDate);
		return this;
	}

	public HttpCookie withMaxAge(int maxAge) {
		// %x31-39 ; digits 1 through 9
		setMaxAge(maxAge);
		return this;
	}

	public HttpCookie withMaxAge(Duration maxAge) {
		setMaxAge(maxAge);
		return this;
	}

	public HttpCookie withDomain(String domain) {
		// https://tools.ietf.org/html/rfc1034#section-3.5
		setDomain(domain);
		return this;
	}

	public HttpCookie withPath(String path) {
		// <any CHAR except CTLs or ";">
		setPath(path);
		return this;
	}

	public HttpCookie withSecure(boolean secure) {
		setSecure(secure);
		return this;
	}

	public HttpCookie withHttpOnly(boolean httpOnly) {
		setHttpOnly(httpOnly);
		return this;
	}

	public HttpCookie withExtension(String extension) {
		// any CHAR except CTLs or ";"
		setExtension(extension);
		return this;
	}
	// endregion

	public String getName() {
		return name;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Instant getExpirationDate() {
		return Instant.ofEpochSecond(expirationDate);
	}

	public void setExpirationDate(Instant expirationDate) {
		this.expirationDate = expirationDate.getEpochSecond();
	}

	public int getMaxAge() {
		return maxAge;
	}

	public void setMaxAge(int maxAge) {
		this.maxAge = maxAge;
	}

	public void setMaxAge(Duration maxAge) {
		this.maxAge = (int) maxAge.getSeconds();
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public boolean isSecure() {
		return secure;
	}

	public void setSecure(boolean secure) {
		this.secure = secure;
	}

	public boolean isHttpOnly() {
		return httpOnly;
	}

	public void setHttpOnly(boolean httpOnly) {
		this.httpOnly = httpOnly;
	}

	public String getExtension() {
		return extension;
	}

	public void setExtension(String extension) {
		this.extension = extension;
	}
	// endregion

	static void decodeFull(byte[] bytes, int pos, int end, List<HttpCookie> cookies) throws MalformedHttpException {
		try {
			HttpCookie cookie = new HttpCookie("", "", "/");
			while (pos < end) {
				pos = skipSpaces(bytes, pos, end);
				int keyStart = pos;
				while (pos < end && bytes[pos] != ';') {
					pos++;
				}
				int valueEnd = pos;
				int equalSign = -1;
				for (int i = keyStart; i < valueEnd; i++) {
					if (bytes[i] == '=') {
						equalSign = i;
						break;
					}
				}
				AvHandler handler = getCookieHandler(hashCodeLowerCaseAscii
						(bytes, keyStart, (equalSign == -1 ? valueEnd : equalSign) - keyStart));
				if (equalSign == -1 && handler == null) {
					cookie.setExtension(decodeAscii(bytes, keyStart, valueEnd - keyStart));
				} else if (handler == null) {
					String key = decodeAscii(bytes, keyStart, equalSign - keyStart);
					String value;
					if (bytes[equalSign + 1] == '\"' && bytes[valueEnd - 1] == '\"') {
						value = decodeAscii(bytes, equalSign + 2, valueEnd - equalSign - 3);
					} else {
						value = decodeAscii(bytes, equalSign + 1, valueEnd - equalSign - 1);
					}
					cookie = new HttpCookie(key, value, "/");
					cookies.add(cookie);
				} else {
					handler.handle(cookie, bytes, equalSign + 1, valueEnd);
				}
				pos = valueEnd + 1;
			}
		} catch (RuntimeException e) {
			throw new MalformedHttpException("Failed to decode cookies", e);
		}
	}

	static void renderSimple(List<HttpCookie> cookies, ByteBuf buf) {
		int pos = renderSimple(cookies, buf.array(), buf.tail());
		buf.tail(pos);
	}

	static int renderSimple(List<HttpCookie> cookies, byte[] bytes, int pos) {
		for (int i = 0; i < cookies.size(); i++) {
			HttpCookie cookie = cookies.get(i);
			pos += encodeAscii(bytes, pos, cookie.name);

			if (cookie.value != null) {
				bytes[pos++] = EQUALS;
				pos += encodeAscii(bytes, pos, cookie.value);
			}

			if (i != cookies.size() - 1) {
				bytes[pos++] = SEMICOLON;
				bytes[pos++] = SP;
			}
		}
		return pos;
	}

	static void decodeSimple(byte[] bytes, int pos, int end, List<HttpCookie> cookies) throws MalformedHttpException {
		try {
			while (pos < end) {
				pos = skipSpaces(bytes, pos, end);
				int keyStart = pos;
				while (pos < end && !(bytes[pos] == ';' || bytes[pos] == ',')) {
					pos++;
				}
				int valueEnd = pos;
				int equalSign = -1;
				for (int i = keyStart; i < valueEnd; i++) {
					if (bytes[i] == '=') {
						equalSign = i;
						break;
					}
				}

				if (equalSign == -1) {
					String key = decodeAscii(bytes, keyStart, valueEnd - keyStart);
					cookies.add(new HttpCookie(key, null));
				} else {
					String key = decodeAscii(bytes, keyStart, equalSign - keyStart);
					String value;
					if (bytes[equalSign + 1] == '\"' && bytes[valueEnd - 1] == '\"') {
						value = decodeAscii(bytes, equalSign + 2, valueEnd - equalSign - 3);
					} else {
						value = decodeAscii(bytes, equalSign + 1, valueEnd - equalSign - 1);
					}

					cookies.add(new HttpCookie(key, value));
				}

				pos = valueEnd + 1;
			}
		} catch (RuntimeException e) {
			throw new MalformedHttpException("Failed to decode cookies", e);
		}
	}

	void renderFull(ByteBuf buf) {
		int pos = renderFull(buf.array(), buf.tail());
		buf.tail(pos);
	}

	int renderFull(byte[] container, int pos) {
		pos += encodeAscii(container, pos, name);
		container[pos++] = EQUALS;
		if (value != null) {
			pos += encodeAscii(container, pos, value);
		}
		if (expirationDate != -1) {
			container[pos++] = SEMICOLON;
			container[pos++] = SP;
			for (byte expireByte : EXPIRES) {
				container[pos++] = expireByte;
			}
			container[pos++] = EQUALS;
			pos = HttpDate.render(expirationDate, container, pos);
		}
		if (maxAge >= 0) {
			container[pos++] = SEMICOLON;
			container[pos++] = SP;
			for (byte maxAgeByte : MAX_AGE) {
				container[pos++] = maxAgeByte;
			}
			container[pos++] = EQUALS;
			pos += encodePositiveInt(container, pos, maxAge);
		}
		if (domain != null) {
			container[pos++] = SEMICOLON;
			container[pos++] = SP;
			for (byte domainByte : DOMAIN) {
				container[pos++] = domainByte;
			}
			container[pos++] = EQUALS;
			pos += encodeAscii(container, pos, domain);
		}
		if (!(path == null || path.equals(""))) {
			container[pos++] = SEMICOLON;
			container[pos++] = SP;
			for (byte pathByte : PATH) {
				container[pos++] = pathByte;
			}
			container[pos++] = EQUALS;
			pos += encodeAscii(container, pos, path);
		}
		if (secure) {
			container[pos++] = SEMICOLON;
			container[pos++] = SP;
			for (byte secureByte : SECURE) {
				container[pos++] = secureByte;
			}
		}
		if (httpOnly) {
			container[pos++] = SEMICOLON;
			container[pos++] = SP;
			for (byte httpOnlyByte : HTTPONLY) {
				container[pos++] = httpOnlyByte;
			}
		}
		if (extension != null) {
			container[pos++] = SEMICOLON;
			container[pos++] = SP;
			pos += encodeAscii(container, pos, extension);
		}
		return pos;
	}

	private static AvHandler getCookieHandler(int hash) {
		switch (hash) {
			case EXPIRES_HC:
				return new AvHandler() {
					@Override
					protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) throws MalformedHttpException {
						cookie.setExpirationDate(decodeExpirationDate(bytes, start));
					}
				};
			case MAX_AGE_HC:
				return new AvHandler() {
					@Override
					protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) throws MalformedHttpException {
						cookie.setMaxAge(decodeMaxAge(bytes, start, end));
					}
				};
			case DOMAIN_HC:
				return new AvHandler() {
					@Override
					protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) {
						cookie.setDomain(decodeAscii(bytes, start, end - start));
					}
				};
			case PATH_HC:
				return new AvHandler() {
					@Override
					protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) {
						cookie.setPath(decodeAscii(bytes, start, end - start));
					}
				};
			case SECURE_HC:
				return new AvHandler() {
					@Override
					protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) {
						cookie.setSecure(true);
					}
				};
			case HTTP_ONLY_HC:
				return new AvHandler() {
					@Override
					protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) {
						cookie.setHttpOnly(true);
					}
				};
			default:
				return null;
		}
	}

	private static Instant decodeExpirationDate(byte[] bytes, int start) throws MalformedHttpException {
		return Instant.ofEpochSecond(HttpDate.decode(bytes, start));
	}

	private static Duration decodeMaxAge(byte[] bytes, int start, int end) throws MalformedHttpException {
		return Duration.ofSeconds(trimAndDecodePositiveInt(bytes, start, end - start));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		HttpCookie that = (HttpCookie) o;
		return maxAge == that.maxAge &&
				secure == that.secure &&
				httpOnly == that.httpOnly &&
				expirationDate == that.expirationDate &&
				Objects.equals(name, that.name) &&
				Objects.equals(value, that.value) &&
				Objects.equals(domain, that.domain) &&
				Objects.equals(path, that.path) &&
				Objects.equals(extension, that.extension);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, value, expirationDate, maxAge, domain, path, secure, httpOnly, extension);
	}

	@Override
	public String toString() {
		return "HttpCookie{" +
				"name='" + name + '\'' +
				", value='" + value + '\'' + '}';
	}
	// endregion
}
