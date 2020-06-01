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
import io.activej.common.parse.ParseException;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.List;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.http.HttpUtils.trimAndDecodePositiveInt;
import static java.util.Arrays.asList;

@SuppressWarnings("WeakerAccess")
public abstract class HttpHeaderValue {
	abstract int estimateSize();

	abstract void writeTo(@NotNull ByteBuf buf);

	@Override
	@NotNull
	public abstract String toString();

	@NotNull
	public static HttpHeaderValue of(@NotNull String string) {
		return new HttpHeaderValueOfString(string);
	}

	@NotNull
	public static HttpHeaderValue ofBytes(@NotNull byte[] array, int offset, int size) {
		return new HttpHeaderValueOfBytes(array, offset, size);
	}

	@NotNull
	public static HttpHeaderValue ofBytes(@NotNull byte[] array) {
		return ofBytes(array, 0, array.length);
	}

	@NotNull
	public static HttpHeaderValue ofDecimal(int value) {
		return new HttpHeaderValueOfUnsignedDecimal(value);
	}

	@NotNull
	public static HttpHeaderValue ofAcceptCharsets(@NotNull List<AcceptCharset> charsets) {
		return new HttpHeaderValueOfCharsets(charsets);
	}

	@NotNull
	public static HttpHeaderValue ofAcceptCharsets(@NotNull AcceptCharset... charsets) {
		return ofAcceptCharsets(asList(charsets));
	}

	@NotNull
	public static HttpHeaderValue ofInstant(@NotNull Instant date) {
		return new HttpHeaderValueOfDate(date.getEpochSecond());
	}

	@NotNull
	public static HttpHeaderValue ofTimestamp(long timestamp) {
		return new HttpHeaderValueOfDate(timestamp / 1000L);
	}

	@NotNull
	public static HttpHeaderValue ofAcceptMediaTypes(@NotNull List<AcceptMediaType> type) {
		return new HttpHeaderValueOfAcceptMediaTypes(type);
	}

	@NotNull
	public static HttpHeaderValue ofAcceptMediaTypes(@NotNull AcceptMediaType... types) {
		return ofAcceptMediaTypes(asList(types));
	}

	@NotNull
	public static HttpHeaderValue ofContentType(@NotNull ContentType type) {
		return new HttpHeaderValueOfContentType(type);
	}

	public static int toPositiveInt(@NotNull ByteBuf buf) throws ParseException {
		return trimAndDecodePositiveInt(buf.array(), buf.head(), buf.readRemaining());
	}

	@NotNull
	public static ContentType toContentType(@NotNull ByteBuf buf) throws ParseException {
		return ContentType.parse(buf.array(), buf.head(), buf.readRemaining());
	}

	@NotNull
	public static Instant toInstant(@NotNull ByteBuf buf) throws ParseException {
		return Instant.ofEpochSecond(HttpDate.parse(buf.array(), buf.head()));
	}

	@NotNull
	public ByteBuf getBuf() {
		int estimatedSize = estimateSize();
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[estimatedSize]);
		writeTo(buf);
		return buf;
	}

	@FunctionalInterface
	public interface ParserIntoList<T> {
		void parse(@NotNull ByteBuf buf, @NotNull List<T> into) throws ParseException;
	}

	public static void toAcceptContentTypes(@NotNull ByteBuf buf, @NotNull List<AcceptMediaType> into) throws ParseException {
		AcceptMediaType.parse(buf.array(), buf.head(), buf.readRemaining(), into);
	}

	public static void toAcceptCharsets(@NotNull ByteBuf buf, @NotNull List<AcceptCharset> into) throws ParseException {
		AcceptCharset.parse(buf.array(), buf.head(), buf.readRemaining(), into);
	}

	static void toSimpleCookies(@NotNull ByteBuf buf, @NotNull List<HttpCookie> into) throws ParseException {
		HttpCookie.parseSimple(buf.array(), buf.head(), buf.tail(), into);
	}

	static void toFullCookies(@NotNull ByteBuf buf, @NotNull List<HttpCookie> into) throws ParseException {
		HttpCookie.parseFull(buf.array(), buf.head(), buf.tail(), into);
	}

	static final class HttpHeaderValueOfContentType extends HttpHeaderValue {
		@NotNull
		private final ContentType type;

		HttpHeaderValueOfContentType(@NotNull ContentType type) {
			this.type = type;
		}

		@Override
		public int estimateSize() {
			return type.size();
		}

		@Override
		public void writeTo(@NotNull ByteBuf buf) {
			ContentType.render(type, buf);
		}

		@NotNull
		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			ContentType.render(type, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	static final class HttpHeaderValueOfAcceptMediaTypes extends HttpHeaderValue {
		@NotNull
		private final List<AcceptMediaType> types;

		HttpHeaderValueOfAcceptMediaTypes(@NotNull List<AcceptMediaType> types) {
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
		public void writeTo(@NotNull ByteBuf buf) {
			AcceptMediaType.render(types, buf);
		}

		@NotNull
		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			AcceptMediaType.render(types, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	static final class HttpHeaderValueOfSimpleCookies extends HttpHeaderValue {
		@NotNull
		final List<HttpCookie> cookies;

		HttpHeaderValueOfSimpleCookies(@NotNull List<HttpCookie> cookies) {
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
		void writeTo(@NotNull ByteBuf buf) {
			HttpCookie.renderSimple(cookies, buf);
		}

		@NotNull
		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			HttpCookie.renderSimple(cookies, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	static final class HttpHeaderValueOfSetCookies extends HttpHeaderValue {
		@NotNull
		final HttpCookie cookie;

		HttpHeaderValueOfSetCookies(@NotNull HttpCookie cookie) {
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
		void writeTo(@NotNull ByteBuf buf) {
			cookie.renderFull(buf);
		}

		@NotNull
		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			cookie.renderFull(buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	static final class HttpHeaderValueOfCharsets extends HttpHeaderValue {
		@NotNull
		private final List<AcceptCharset> charsets;

		HttpHeaderValueOfCharsets(@NotNull List<AcceptCharset> charsets) {
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
		void writeTo(@NotNull ByteBuf buf) {
			AcceptCharset.render(charsets, buf);
		}

		@NotNull
		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			AcceptCharset.render(charsets, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	static final class HttpHeaderValueOfDate extends HttpHeaderValue {
		private final long epochSeconds;

		HttpHeaderValueOfDate(long epochSeconds) {
			this.epochSeconds = epochSeconds;
		}

		@Override
		int estimateSize() {
			return 29;
		}

		@Override
		void writeTo(@NotNull ByteBuf buf) {
			HttpDate.render(epochSeconds, buf);
		}

		@NotNull
		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			HttpDate.render(epochSeconds, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	static final class HttpHeaderValueOfUnsignedDecimal extends HttpHeaderValue {
		private final int value;

		HttpHeaderValueOfUnsignedDecimal(int value) {
			this.value = value;
		}

		@Override
		int estimateSize() {
			return 10; // Integer.toString(Integer.MAX_VALUE).length();
		}

		@Override
		void writeTo(@NotNull ByteBuf buf) {
			putPositiveInt(buf, value);
		}

		@NotNull
		@Override
		public String toString() {
			return Integer.toString(value);
		}
	}

	static final class HttpHeaderValueOfString extends HttpHeaderValue {
		@NotNull
		private final String string;

		HttpHeaderValueOfString(@NotNull String string) {
			this.string = string;
		}

		@Override
		int estimateSize() {
			return string.length();
		}

		@Override
		void writeTo(@NotNull ByteBuf buf) {
			putAscii(buf, string);
		}

		@NotNull
		@Override
		public String toString() {
			return string;
		}
	}

	static final class HttpHeaderValueOfBytes extends HttpHeaderValue {
		@NotNull
		private final byte[] array;
		private final int offset;
		private final int size;

		HttpHeaderValueOfBytes(@NotNull byte[] array, int offset, int size) {
			this.array = array;
			this.offset = offset;
			this.size = size;
		}

		@Override
		int estimateSize() {
			return size;
		}

		@Override
		void writeTo(@NotNull ByteBuf buf) {
			buf.put(array, offset, size);
		}

		@NotNull
		@Override
		public ByteBuf getBuf() {
			return ByteBuf.wrap(array, offset, offset + size);
		}

		@NotNull
		@Override
		public String toString() {
			return decodeAscii(array, offset, size);
		}
	}

}
