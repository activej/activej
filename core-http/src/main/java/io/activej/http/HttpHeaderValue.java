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
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufStrings;

import java.time.Instant;
import java.util.List;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.http.HttpUtils.trimAndDecodePositiveInt;

@SuppressWarnings("WeakerAccess")
public abstract class HttpHeaderValue {
	abstract int estimateSize();

	abstract int writeTo(byte[] array, int offset);

	@Override
	public abstract String toString();

	public static HttpHeaderValue of(String string) {
		return new HttpHeaderValueOfString(string);
	}

	public static HttpHeaderValue ofBytes(byte[] array, int offset, int size) {
		return new HttpHeaderValueOfBytes(array, offset, size);
	}

	public static HttpHeaderValue ofBytes(byte[] array) {
		return ofBytes(array, 0, array.length);
	}

	public static HttpHeaderValue ofDecimal(int value) {
		return new HttpHeaderValueOfUnsignedDecimal(value);
	}

	public static HttpHeaderValue ofAcceptCharsets(List<AcceptCharset> charsets) {
		return new HttpHeaderValueOfCharsets(charsets);
	}

	public static HttpHeaderValue ofAcceptCharsets(AcceptCharset... charsets) {
		return ofAcceptCharsets(List.of(charsets));
	}

	public static HttpHeaderValue ofInstant(Instant date) {
		return new HttpHeaderValueOfDate(date.getEpochSecond());
	}

	public static HttpHeaderValue ofTimestamp(long timestamp) {
		return new HttpHeaderValueOfDate(timestamp / 1000L);
	}

	public static HttpHeaderValue ofAcceptMediaTypes(List<AcceptMediaType> type) {
		return new HttpHeaderValueOfAcceptMediaTypes(type);
	}

	public static HttpHeaderValue ofAcceptMediaTypes(AcceptMediaType... types) {
		return ofAcceptMediaTypes(List.of(types));
	}

	public static HttpHeaderValue ofContentType(ContentType type) {
		return new HttpHeaderValueOfContentType(type);
	}

	public static int toPositiveInt(ByteBuf buf) throws MalformedHttpException {
		return trimAndDecodePositiveInt(buf.array(), buf.head(), buf.readRemaining());
	}

	public static ContentType toContentType(ByteBuf buf) throws MalformedHttpException {
		try {
			return ContentType.decode(buf.array(), buf.head(), buf.readRemaining());
		} catch (RuntimeException e) {
			throw new MalformedHttpException("Failed to decode content-type", e);
		}
	}

	public static Instant toInstant(ByteBuf buf) throws MalformedHttpException {
		try {
			return Instant.ofEpochSecond(HttpDate.decode(buf.array(), buf.head()));
		} catch (RuntimeException e) {
			throw new MalformedHttpException("Failed to decode date", e);
		}
	}

	public ByteBuf getBuf() {
		int estimatedSize = estimateSize();
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[estimatedSize]);
		int pos = writeTo(buf.array(), 0);
		buf.tail(pos);
		return buf;
	}

	@FunctionalInterface
	public interface DecoderIntoList<T> {
		void decode(ByteBuf buf, List<T> into) throws MalformedHttpException;
	}

	public static void toAcceptContentTypes(ByteBuf buf, List<AcceptMediaType> into) throws MalformedHttpException {
		try {
			AcceptMediaType.decode(buf.array(), buf.head(), buf.readRemaining(), into);
		} catch (RuntimeException e) {
			throw new MalformedHttpException("Failed to decode accept-content", e);
		}
	}

	public static void toAcceptCharsets(ByteBuf buf, List<AcceptCharset> into) throws MalformedHttpException {
		try {
			AcceptCharset.decode(buf.array(), buf.head(), buf.readRemaining(), into);
		} catch (RuntimeException e) {
			throw new MalformedHttpException("Failed to decode accept-charset", e);
		}
	}

	static void toSimpleCookies(ByteBuf buf, List<HttpCookie> into) throws MalformedHttpException {
		try {
			HttpCookie.decodeSimple(buf.array(), buf.head(), buf.tail(), into);
		} catch (RuntimeException e) {
			throw new MalformedHttpException("Failed to decode cookies", e);
		}
	}

	static void toFullCookies(ByteBuf buf, List<HttpCookie> into) throws MalformedHttpException {
		try {
			HttpCookie.decodeFull(buf.array(), buf.head(), buf.tail(), into);
		} catch (RuntimeException e) {
			throw new MalformedHttpException("Failed to decode cookies", e);
		}
	}

	public static final class HttpHeaderValueOfContentType extends HttpHeaderValue {
		private final ContentType type;

		HttpHeaderValueOfContentType(ContentType type) {
			this.type = type;
		}

		@Override
		public int estimateSize() {
			return type.size();
		}

		@Override
		public int writeTo(byte[] array, int offset) {
			return ContentType.render(type, array, offset);
		}

		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			ContentType.render(type, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	public static final class HttpHeaderValueOfAcceptMediaTypes extends HttpHeaderValue {
		private final List<AcceptMediaType> types;

		HttpHeaderValueOfAcceptMediaTypes(List<AcceptMediaType> types) {
			this.types = types;
		}

		@Override
		public int estimateSize() {
			int size = 0;
			for (AcceptMediaType type : types) {
				size += type.estimateSize() + 2;
			}
			return size;
		}

		@Override
		public int writeTo(byte[] array, int offset) {
			return AcceptMediaType.render(types, array, offset);
		}

		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			AcceptMediaType.render(types, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	public static final class HttpHeaderValueOfSimpleCookies extends HttpHeaderValue {
		final List<HttpCookie> cookies;

		HttpHeaderValueOfSimpleCookies(List<HttpCookie> cookies) {
			this.cookies = cookies;
		}

		@Override
		int estimateSize() {
			int size = 0;
			for (HttpCookie cookie : cookies) {
				size += cookie.getName().length();
				size += cookie.getValue() == null ? 0 : cookie.getValue().length() + 1;
			}
			size += (cookies.size() - 1) * 2; // semicolons and spaces
			return size;
		}

		@Override
		int writeTo(byte[] array, int offset) {
			return HttpCookie.renderSimple(cookies, array, offset);
		}

		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			HttpCookie.renderSimple(cookies, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	public static final class HttpHeaderValueOfSetCookies extends HttpHeaderValue {
		final HttpCookie cookie;

		HttpHeaderValueOfSetCookies(HttpCookie cookie) {
			this.cookie = cookie;
		}

		@Override
		int estimateSize() {
			int size = 0;
			size += cookie.getName().length();
			size += cookie.getValue() == null ? 0 : cookie.getValue().length() + 1;
			size += cookie.getDomain() == null ? 0 : cookie.getDomain().length() + 10;
			size += cookie.getPath() == null ? 0 : cookie.getPath().length() + 6;
			size += cookie.getExtension() == null ? 0 : cookie.getExtension().length();
			size += 102;
			return size;
		}

		@Override
		int writeTo(byte[] array, int offset) {
			return cookie.renderFull(array, offset);
		}

		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			cookie.renderFull(buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	public static final class HttpHeaderValueOfCharsets extends HttpHeaderValue {
		private final List<AcceptCharset> charsets;

		HttpHeaderValueOfCharsets(List<AcceptCharset> charsets) {
			this.charsets = charsets;
		}

		@Override
		int estimateSize() {
			int size = 0;
			for (AcceptCharset charset : charsets) {
				size += charset.estimateSize() + 2;
			}
			return size;
		}

		@Override
		int writeTo(byte[] array, int offset) {
			return AcceptCharset.render(charsets, array, offset);
		}

		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			AcceptCharset.render(charsets, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	public static final class HttpHeaderValueOfDate extends HttpHeaderValue {
		private final long epochSeconds;

		HttpHeaderValueOfDate(long epochSeconds) {
			this.epochSeconds = epochSeconds;
		}

		@Override
		int estimateSize() {
			return 29;
		}

		@Override
		int writeTo(byte[] array, int offset) {
			return HttpDate.render(epochSeconds, array, offset);
		}

		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			HttpDate.render(epochSeconds, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	public static final class HttpHeaderValueOfUnsignedDecimal extends HttpHeaderValue {
		private final int value;

		HttpHeaderValueOfUnsignedDecimal(int value) {
			this.value = value;
		}

		@Override
		int estimateSize() {
			return 10; // Integer.toString(Integer.MAX_VALUE).length();
		}

		@Override
		int writeTo(byte[] array, int offset) {
			return offset + encodePositiveInt(array, offset, value);
		}

		@Override
		public String toString() {
			return Integer.toString(value);
		}
	}

	public static final class HttpHeaderValueOfString extends HttpHeaderValue {
		private final String string;

		HttpHeaderValueOfString(String string) {
			this.string = string;
		}

		@Override
		int estimateSize() {
			return string.length();
		}

		@Override
		int writeTo(byte[] array, int offset) {
			return offset + encodeAscii(array, offset, string);
		}

		@Override
		public String toString() {
			return string;
		}
	}

	public static final class HttpHeaderValueOfBytes extends HttpHeaderValue {
		private final byte[] array;
		private final int offset;
		private final int size;

		HttpHeaderValueOfBytes(byte[] array, int offset, int size) {
			this.array = array;
			this.offset = offset;
			this.size = size;
		}

		@Override
		int estimateSize() {
			return size;
		}

		@Override
		int writeTo(byte[] array, int offset) {
			if (this.array.length < 10) {
				for (byte b : this.array) {
					array[offset++] = b;
				}
				return offset;
			} else {
				System.arraycopy(this.array, this.offset, array, offset, size);
				return offset + size;
			}
		}

		@Override
		public ByteBuf getBuf() {
			return ByteBuf.wrap(array, offset, offset + size);
		}

		@Override
		public String toString() {
			return decodeAscii(array, offset, size);
		}
	}

}
