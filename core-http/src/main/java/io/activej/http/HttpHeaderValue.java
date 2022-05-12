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
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.List;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.http.HttpUtils.trimAndDecodePositiveInt;

@SuppressWarnings("WeakerAccess")
public abstract class HttpHeaderValue {
	abstract int estimateSize();

	abstract int writeTo(byte[] array, int offset);

	@Override
	public abstract @NotNull String toString();

	public static @NotNull HttpHeaderValue of(@NotNull String string) {
		return new HttpHeaderValueOfString(string);
	}

	public static @NotNull HttpHeaderValue ofBytes(byte[] array, int offset, int size) {
		return new HttpHeaderValueOfBytes(array, offset, size);
	}

	public static @NotNull HttpHeaderValue ofBytes(byte[] array) {
		return ofBytes(array, 0, array.length);
	}

	public static @NotNull HttpHeaderValue ofDecimal(int value) {
		return new HttpHeaderValueOfUnsignedDecimal(value);
	}

	public static @NotNull HttpHeaderValue ofAcceptCharsets(@NotNull List<AcceptCharset> charsets) {
		return new HttpHeaderValueOfCharsets(charsets);
	}

	public static @NotNull HttpHeaderValue ofAcceptCharsets(AcceptCharset... charsets) {
		return ofAcceptCharsets(List.of(charsets));
	}

	public static @NotNull HttpHeaderValue ofInstant(@NotNull Instant date) {
		return new HttpHeaderValueOfDate(date.getEpochSecond());
	}

	public static @NotNull HttpHeaderValue ofTimestamp(long timestamp) {
		return new HttpHeaderValueOfDate(timestamp / 1000L);
	}

	public static @NotNull HttpHeaderValue ofAcceptMediaTypes(@NotNull List<AcceptMediaType> type) {
		return new HttpHeaderValueOfAcceptMediaTypes(type);
	}

	public static @NotNull HttpHeaderValue ofAcceptMediaTypes(AcceptMediaType... types) {
		return ofAcceptMediaTypes(List.of(types));
	}

	public static @NotNull HttpHeaderValue ofContentType(@NotNull ContentType type) {
		return new HttpHeaderValueOfContentType(type);
	}

	public static int toPositiveInt(@NotNull ByteBuf buf) throws MalformedHttpException {
		return trimAndDecodePositiveInt(buf.array(), buf.head(), buf.readRemaining());
	}

	public static @NotNull ContentType toContentType(@NotNull ByteBuf buf) throws MalformedHttpException {
		return ContentType.decode(buf.array(), buf.head(), buf.readRemaining());
	}

	public static @NotNull Instant toInstant(@NotNull ByteBuf buf) throws MalformedHttpException {
		return Instant.ofEpochSecond(HttpDate.decode(buf.array(), buf.head()));
	}

	public @NotNull ByteBuf getBuf() {
		int estimatedSize = estimateSize();
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[estimatedSize]);
		int pos = writeTo(buf.array(), 0);
		buf.tail(pos);
		return buf;
	}

	@FunctionalInterface
	public interface DecoderIntoList<T> {
		void decode(@NotNull ByteBuf buf, @NotNull List<T> into) throws MalformedHttpException;
	}

	public static void toAcceptContentTypes(@NotNull ByteBuf buf, @NotNull List<AcceptMediaType> into) throws MalformedHttpException {
		AcceptMediaType.decode(buf.array(), buf.head(), buf.readRemaining(), into);
	}

	public static void toAcceptCharsets(@NotNull ByteBuf buf, @NotNull List<AcceptCharset> into) throws MalformedHttpException {
		AcceptCharset.decode(buf.array(), buf.head(), buf.readRemaining(), into);
	}

	static void toSimpleCookies(@NotNull ByteBuf buf, @NotNull List<HttpCookie> into) throws MalformedHttpException {
		HttpCookie.decodeSimple(buf.array(), buf.head(), buf.tail(), into);
	}

	static void toFullCookies(@NotNull ByteBuf buf, @NotNull List<HttpCookie> into) throws MalformedHttpException {
		HttpCookie.decodeFull(buf.array(), buf.head(), buf.tail(), into);
	}

	static final class HttpHeaderValueOfContentType extends HttpHeaderValue {
		private final @NotNull ContentType type;

		HttpHeaderValueOfContentType(@NotNull ContentType type) {
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
		public @NotNull String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			ContentType.render(type, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	static final class HttpHeaderValueOfAcceptMediaTypes extends HttpHeaderValue {
		private final @NotNull List<AcceptMediaType> types;

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
		public int writeTo(byte[] array, int offset) {
			return AcceptMediaType.render(types, array, offset);
		}

		@Override
		public @NotNull String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			AcceptMediaType.render(types, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	static final class HttpHeaderValueOfSimpleCookies extends HttpHeaderValue {
		final @NotNull List<HttpCookie> cookies;

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
		int writeTo(byte[] array, int offset) {
			return HttpCookie.renderSimple(cookies, array, offset);
		}

		@Override
		public @NotNull String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			HttpCookie.renderSimple(cookies, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	static final class HttpHeaderValueOfSetCookies extends HttpHeaderValue {
		final @NotNull HttpCookie cookie;

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
		int writeTo(byte[] array, int offset) {
			return cookie.renderFull(array, offset);
		}

		@Override
		public @NotNull String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			cookie.renderFull(buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	static final class HttpHeaderValueOfCharsets extends HttpHeaderValue {
		private final @NotNull List<AcceptCharset> charsets;

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
		int writeTo(byte[] array, int offset) {
			return AcceptCharset.render(charsets, array, offset);
		}

		@Override
		public @NotNull String toString() {
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
		int writeTo(byte[] array, int offset) {
			return HttpDate.render(epochSeconds, array, offset);
		}

		@Override
		public @NotNull String toString() {
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
		int writeTo(byte[] array, int offset) {
			return offset + encodePositiveInt(array, offset, value);
		}

		@Override
		public @NotNull String toString() {
			return Integer.toString(value);
		}
	}

	static final class HttpHeaderValueOfString extends HttpHeaderValue {
		private final @NotNull String string;

		HttpHeaderValueOfString(@NotNull String string) {
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
		public @NotNull String toString() {
			return string;
		}
	}

	static final class HttpHeaderValueOfBytes extends HttpHeaderValue {
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
		public @NotNull ByteBuf getBuf() {
			return ByteBuf.wrap(array, offset, offset + size);
		}

		@Override
		public @NotNull String toString() {
			return decodeAscii(array, offset, size);
		}
	}

}
