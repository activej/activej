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
import io.activej.common.Checks;
import io.activej.common.initializer.WithInitializer;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.http.HttpHeaderValue.HttpHeaderValueOfSetCookies;
import io.activej.promise.Promisable;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.bytebuf.ByteBufStrings.putPositiveInt;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static io.activej.http.ContentTypes.*;
import static io.activej.http.HttpHeaderValue.ofContentType;
import static io.activej.http.HttpHeaders.*;
import static io.activej.http.HttpVersion.HTTP_1_1;
import static io.activej.http.MediaTypes.OCTET_STREAM;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Represents HTTP response for {@link HttpRequest}. After handling {@code HttpResponse} will be recycled, so you cannot
 * usi it afterwards.
 */
public final class HttpResponse extends HttpMessage implements Promisable<HttpResponse>, WithInitializer<HttpResponse> {
	private static final boolean CHECKS = Checks.isEnabled(HttpResponse.class);

	private static final byte[] HTTP11_BYTES = encodeAscii("HTTP/1.1 ");
	private static final byte[] CODE_ERROR_BYTES = encodeAscii(" Error");
	private static final byte[] CODE_OK_BYTES = encodeAscii(" OK");
	private static final byte[] CODE_100_BYTES = encodeAscii("HTTP/1.1 100 Continue");
	private static final byte[] CODE_101_BYTES = encodeAscii("HTTP/1.1 101 Switching protocols");
	private static final byte[] CODE_102_BYTES = encodeAscii("HTTP/1.1 102 Processing");
	private static final byte[] CODE_103_BYTES = encodeAscii("HTTP/1.1 103 Early Hints");
	private static final byte[] CODE_200_BYTES = encodeAscii("HTTP/1.1 200 OK");
	private static final byte[] CODE_201_BYTES = encodeAscii("HTTP/1.1 201 Created");
	private static final byte[] CODE_202_BYTES = encodeAscii("HTTP/1.1 202 Accepted");
	private static final byte[] CODE_203_BYTES = encodeAscii("HTTP/1.1 203 Non-Authoritative Information");
	private static final byte[] CODE_204_BYTES = encodeAscii("HTTP/1.1 204 No Content");
	private static final byte[] CODE_205_BYTES = encodeAscii("HTTP/1.1 205 Reset Content");
	private static final byte[] CODE_206_BYTES = encodeAscii("HTTP/1.1 206 Partial Content");
	private static final byte[] CODE_207_BYTES = encodeAscii("HTTP/1.1 207 Multi-Status");
	private static final byte[] CODE_208_BYTES = encodeAscii("HTTP/1.1 208 Already Reported");
	private static final byte[] CODE_226_BYTES = encodeAscii("HTTP/1.1 226 IM Used");
	private static final byte[] CODE_300_BYTES = encodeAscii("HTTP/1.1 300 Multiple Choices");
	private static final byte[] CODE_301_BYTES = encodeAscii("HTTP/1.1 301 Moved Permanently");
	private static final byte[] CODE_302_BYTES = encodeAscii("HTTP/1.1 302 Found");
	private static final byte[] CODE_303_BYTES = encodeAscii("HTTP/1.1 303 See Other");
	private static final byte[] CODE_304_BYTES = encodeAscii("HTTP/1.1 304 Not Modified");
	private static final byte[] CODE_305_BYTES = encodeAscii("HTTP/1.1 305 Use Proxy");
	private static final byte[] CODE_307_BYTES = encodeAscii("HTTP/1.1 307 Temporary Redirect");
	private static final byte[] CODE_308_BYTES = encodeAscii("HTTP/1.1 308 Permanent Redirect");
	private static final byte[] CODE_400_BYTES = encodeAscii("HTTP/1.1 400 Bad Request");
	private static final byte[] CODE_401_BYTES = encodeAscii("HTTP/1.1 401 Unauthorized");
	private static final byte[] CODE_402_BYTES = encodeAscii("HTTP/1.1 402 Payment required");
	private static final byte[] CODE_403_BYTES = encodeAscii("HTTP/1.1 403 Forbidden");
	private static final byte[] CODE_404_BYTES = encodeAscii("HTTP/1.1 404 Not Found");
	private static final byte[] CODE_405_BYTES = encodeAscii("HTTP/1.1 405 Method Not Allowed");
	private static final byte[] CODE_406_BYTES = encodeAscii("HTTP/1.1 406 Not Acceptable");
	private static final byte[] CODE_407_BYTES = encodeAscii("HTTP/1.1 407 Proxy Authentication Required");
	private static final byte[] CODE_408_BYTES = encodeAscii("HTTP/1.1 408 Request Timeout");
	private static final byte[] CODE_409_BYTES = encodeAscii("HTTP/1.1 409 Conflict");
	private static final byte[] CODE_410_BYTES = encodeAscii("HTTP/1.1 410 Gone");
	private static final byte[] CODE_411_BYTES = encodeAscii("HTTP/1.1 411 Length Required");
	private static final byte[] CODE_412_BYTES = encodeAscii("HTTP/1.1 412 Precondition Failed");
	private static final byte[] CODE_413_BYTES = encodeAscii("HTTP/1.1 413 Payload Too Large");
	private static final byte[] CODE_414_BYTES = encodeAscii("HTTP/1.1 414 URI Too Long");
	private static final byte[] CODE_415_BYTES = encodeAscii("HTTP/1.1 415 Unsupported Media Type");
	private static final byte[] CODE_416_BYTES = encodeAscii("HTTP/1.1 416 Range Not Satisfiable");
	private static final byte[] CODE_417_BYTES = encodeAscii("HTTP/1.1 417 Expectation Failed");
	private static final byte[] CODE_421_BYTES = encodeAscii("HTTP/1.1 421 Misdirected Request");
	private static final byte[] CODE_422_BYTES = encodeAscii("HTTP/1.1 422 Unprocessable Entity");
	private static final byte[] CODE_423_BYTES = encodeAscii("HTTP/1.1 423 Locked");
	private static final byte[] CODE_424_BYTES = encodeAscii("HTTP/1.1 424 Failed Dependency");
	private static final byte[] CODE_425_BYTES = encodeAscii("HTTP/1.1 425 Too Early");
	private static final byte[] CODE_426_BYTES = encodeAscii("HTTP/1.1 426 Upgrade Required");
	private static final byte[] CODE_428_BYTES = encodeAscii("HTTP/1.1 428 Precondition Required");
	private static final byte[] CODE_429_BYTES = encodeAscii("HTTP/1.1 429 Too Many Requests");
	private static final byte[] CODE_431_BYTES = encodeAscii("HTTP/1.1 431 Request Header Fields Too Large");
	private static final byte[] CODE_451_BYTES = encodeAscii("HTTP/1.1 451 Unavailable For Legal Reasons");
	private static final byte[] CODE_500_BYTES = encodeAscii("HTTP/1.1 500 Internal Server Error");
	private static final byte[] CODE_501_BYTES = encodeAscii("HTTP/1.1 501 Not Implemented");
	private static final byte[] CODE_502_BYTES = encodeAscii("HTTP/1.1 502 Bad Gateway");
	private static final byte[] CODE_503_BYTES = encodeAscii("HTTP/1.1 503 Service Unavailable");
	private static final byte[] CODE_504_BYTES = encodeAscii("HTTP/1.1 504 Gateway Timeout");
	private static final byte[] CODE_505_BYTES = encodeAscii("HTTP/1.1 505 HTTP Version Not Supported");
	private static final byte[] CODE_506_BYTES = encodeAscii("HTTP/1.1 506 Variant Also Negotiates");
	private static final byte[] CODE_507_BYTES = encodeAscii("HTTP/1.1 507 Insufficient Storage");
	private static final byte[] CODE_508_BYTES = encodeAscii("HTTP/1.1 508 Loop Detected");
	private static final byte[] CODE_510_BYTES = encodeAscii("HTTP/1.1 510 Not Extended");
	private static final byte[] CODE_511_BYTES = encodeAscii("HTTP/1.1 511 Network Authentication Required");
	private static final int LONGEST_FIRST_LINE_SIZE = CODE_511_BYTES.length;

	private final HttpClientConnection connection;

	private final int code;

	private @Nullable Map<String, HttpCookie> parsedCookies;

	// region creators
	HttpResponse(HttpVersion version, int code, @Nullable HttpClientConnection connection) {
		super(version);
		this.code = code;
		this.connection = connection;
	}

	HttpResponse(HttpVersion version, int code) {
		this(version, code, null);
	}

	public static Builder builder(int code) {
		if (CHECKS) checkArgument(code >= 100 && code < 600, "Code should be in range [100, 600)");
		return new HttpResponse(HTTP_1_1, code).new Builder();
	}

	public static HttpResponse ofCode(int code) {
		return builder(code).build();
	}

	public static HttpResponse ok200() {
		return Builder.ok200().build();
	}

	public static HttpResponse ok201() {
		return Builder.ok201().build();
	}

	public static HttpResponse ok206() {
		return Builder.ok206().build();
	}

	public static HttpResponse redirect301(String url) {
		return Builder.redirect301(url).build();
	}

	public static HttpResponse redirect302(String url) {
		return Builder.redirect302(url).build();
	}

	public static HttpResponse redirect307(String url) {
		return Builder.redirect307(url).build();
	}

	public static HttpResponse redirect308(String url) {
		return Builder.redirect308(url).build();
	}

	public static HttpResponse unauthorized401(String challenge) {
		return Builder.unauthorized401(challenge).build();
	}

	public static HttpResponse notFound404() {
		return Builder.notFound404().build();
	}

	public static Promise<HttpResponse> file(AsyncFileSliceSupplier downloader, String name, long size, @Nullable String rangeHeader) {
		return file(downloader, name, size, rangeHeader, false);
	}

	public static Promise<HttpResponse> file(AsyncFileSliceSupplier downloader, String name, long size, @Nullable String rangeHeader, boolean inline) {
		Builder builder = builder(rangeHeader == null ? 200 : 206);

		String localName = name.substring(name.lastIndexOf('/') + 1);
		MediaType mediaType = MediaTypes.getByExtension(localName.substring(localName.lastIndexOf('.') + 1));
		if (mediaType == null) {
			mediaType = OCTET_STREAM;
		}

		builder.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(mediaType)));
		builder.withHeader(ACCEPT_RANGES, "bytes");
		builder.withHeader(CONTENT_DISPOSITION, inline ? "inline" : "attachment; filename=\"" + localName + "\"");

		long contentLength, offset;
		if (rangeHeader != null) {
			if (!rangeHeader.startsWith("bytes=")) {
				return Promise.ofException(HttpError.ofCode(416, "Invalid range header (not in bytes)"));
			}
			rangeHeader = rangeHeader.substring(6);
			if (!rangeHeader.matches("(?:\\d+)?-(?:\\d+)?")) {
				return Promise.ofException(HttpError.ofCode(416, "Only single part ranges are allowed"));
			}
			String[] parts = rangeHeader.split("-", 2);
			long endOffset;
			if (parts[0].isEmpty()) {
				if (parts[1].isEmpty()) {
					return Promise.ofException(HttpError.ofCode(416, "Invalid range"));
				}
				offset = size - Long.parseLong(parts[1]);
				endOffset = size;
			} else {
				if (parts[1].isEmpty()) {
					offset = Long.parseLong(parts[0]);
					endOffset = size - 1;
				} else {
					offset = Long.parseLong(parts[0]);
					endOffset = Long.parseLong(parts[1]);
				}
			}
			if (endOffset != -1 && offset > endOffset) {
				return Promise.ofException(HttpError.ofCode(416, "Invalid range"));
			}
			contentLength = endOffset - offset + 1;
			builder.withHeader(CONTENT_RANGE, "bytes " + offset + "-" + endOffset + "/" + size);
		} else {
			contentLength = size;
			offset = 0;
		}
		builder.withHeader(CONTENT_LENGTH, Long.toString(contentLength));
		builder.withBodyStream(ChannelSuppliers.ofPromise(downloader.getFileSlice(offset, contentLength)));
		return Promise.of(builder.build());
	}

	public static Promise<HttpResponse> file(AsyncFileSliceSupplier downloader, String name, long size) {
		return file(downloader, name, size, null);
	}
	// endregion

	public final class Builder extends HttpMessage.Builder<Builder, HttpResponse> {
		private Builder() {}

		public static Builder ok200() {
			return builder(200);
		}

		public static Builder ok201() {
			return builder(201);
		}

		public static Builder ok206() {
			return builder(206);
		}

		public static Builder redirect301(String url) {
			// RFC-7231, section 6.4.2 (https://tools.ietf.org/html/rfc7231#section-6.4.2)
			return builder(301)
					.withHeader(LOCATION, url);
		}

		public static Builder redirect302(String url) {
			// RFC-7231, section 6.4.3 (https://tools.ietf.org/html/rfc7231#section-6.4.3)
			return builder(302)
					.withHeader(LOCATION, url);
		}

		public static Builder redirect307(String url) {
			return builder(307)
					.withHeader(LOCATION, url);
		}

		public static Builder redirect308(String url) {
			// RFC-7238, section 3 (https://tools.ietf.org/html/rfc7238#section-3)
			return builder(308)
					.withHeader(LOCATION, url);
		}

		public static Builder unauthorized401(String challenge) {
			// RFC-7235, section 3.1 (https://tools.ietf.org/html/rfc7235#section-3.1)
			return builder(401)
					.withHeader(WWW_AUTHENTICATE, challenge);
		}

		public static Builder notFound404() {
			return builder(404);
		}

		@Override
		public void addCookies(List<HttpCookie> cookies) {
			for (HttpCookie cookie : cookies) {
				addCookie(cookie);
			}
		}

		@Override
		public void addCookie(HttpCookie cookie) {
			headers.add(SET_COOKIE, new HttpHeaderValueOfSetCookies(cookie));
		}

		public Builder withPlainText(String text) {
			return withHeader(CONTENT_TYPE, ofContentType(PLAIN_TEXT_UTF_8))
					.withBody(text.getBytes(UTF_8));
		}

		public Builder withHtml(String text) {
			return withHeader(CONTENT_TYPE, ofContentType(HTML_UTF_8))
					.withBody(text.getBytes(UTF_8));
		}

		public Builder withJson(String text) {
			return withHeader(CONTENT_TYPE, ofContentType(JSON_UTF_8))
					.withBody(text.getBytes(UTF_8));
		}
	}

	@Override
	public Promise<HttpResponse> promise() {
		return Promise.of(this);
	}

	@FunctionalInterface
	public interface AsyncFileSliceSupplier {

		Promise<ChannelSupplier<ByteBuf>> getFileSlice(long offset, long limit);
	}

	@Override
	boolean isContentLengthExpected() {
		return true;
	}

	public HttpClientConnection getConnection() {
		return connection;
	}

	public int getCode() {
		return code;
	}

	public Map<String, HttpCookie> getCookies() {
		if (CHECKS) checkState(!isRecycled());
		if (parsedCookies != null) {
			return parsedCookies;
		}
		Map<String, HttpCookie> cookies = new LinkedHashMap<>();
		for (HttpCookie cookie : getHeader(SET_COOKIE, HttpHeaderValue::toFullCookies)) {
			cookies.put(cookie.getName(), cookie);
		}
		return parsedCookies = cookies;
	}

	public @Nullable HttpCookie getCookie(String cookie) {
		if (CHECKS) checkState(!isRecycled());
		return getCookies().get(cookie);
	}

	private static void writeCodeMessage(ByteBuf buf, int code) {
		byte[] result;
		switch (code) {
			case 100 -> result = CODE_100_BYTES;
			case 101 -> result = CODE_101_BYTES;
			case 102 -> result = CODE_102_BYTES;
			case 103 -> result = CODE_103_BYTES;
			case 200 -> result = CODE_200_BYTES;
			case 201 -> result = CODE_201_BYTES;
			case 202 -> result = CODE_202_BYTES;
			case 203 -> result = CODE_203_BYTES;
			case 204 -> result = CODE_204_BYTES;
			case 205 -> result = CODE_205_BYTES;
			case 206 -> result = CODE_206_BYTES;
			case 207 -> result = CODE_207_BYTES;
			case 208 -> result = CODE_208_BYTES;
			case 226 -> result = CODE_226_BYTES;
			case 300 -> result = CODE_300_BYTES;
			case 301 -> result = CODE_301_BYTES;
			case 302 -> result = CODE_302_BYTES;
			case 303 -> result = CODE_303_BYTES;
			case 304 -> result = CODE_304_BYTES;
			case 305 -> result = CODE_305_BYTES;
			case 307 -> result = CODE_307_BYTES;
			case 308 -> result = CODE_308_BYTES;
			case 400 -> result = CODE_400_BYTES;
			case 401 -> result = CODE_401_BYTES;
			case 402 -> result = CODE_402_BYTES;
			case 403 -> result = CODE_403_BYTES;
			case 404 -> result = CODE_404_BYTES;
			case 405 -> result = CODE_405_BYTES;
			case 406 -> result = CODE_406_BYTES;
			case 407 -> result = CODE_407_BYTES;
			case 408 -> result = CODE_408_BYTES;
			case 409 -> result = CODE_409_BYTES;
			case 410 -> result = CODE_410_BYTES;
			case 411 -> result = CODE_411_BYTES;
			case 412 -> result = CODE_412_BYTES;
			case 413 -> result = CODE_413_BYTES;
			case 414 -> result = CODE_414_BYTES;
			case 415 -> result = CODE_415_BYTES;
			case 416 -> result = CODE_416_BYTES;
			case 417 -> result = CODE_417_BYTES;
			case 421 -> result = CODE_421_BYTES;
			case 422 -> result = CODE_422_BYTES;
			case 423 -> result = CODE_423_BYTES;
			case 424 -> result = CODE_424_BYTES;
			case 425 -> result = CODE_425_BYTES;
			case 426 -> result = CODE_426_BYTES;
			case 428 -> result = CODE_428_BYTES;
			case 429 -> result = CODE_429_BYTES;
			case 431 -> result = CODE_431_BYTES;
			case 451 -> result = CODE_451_BYTES;
			case 500 -> result = CODE_500_BYTES;
			case 501 -> result = CODE_501_BYTES;
			case 502 -> result = CODE_502_BYTES;
			case 503 -> result = CODE_503_BYTES;
			case 504 -> result = CODE_504_BYTES;
			case 505 -> result = CODE_505_BYTES;
			case 506 -> result = CODE_506_BYTES;
			case 507 -> result = CODE_507_BYTES;
			case 508 -> result = CODE_508_BYTES;
			case 510 -> result = CODE_510_BYTES;
			case 511 -> result = CODE_511_BYTES;
			default -> {
				writeCodeMessage2(buf, code);
				return;
			}
		}
		buf.put(result);
	}

	private static void writeCodeMessage2(ByteBuf buf, int code) {
		buf.put(HTTP11_BYTES);
		putPositiveInt(buf, code);
		if (code >= 400) {
			buf.put(CODE_ERROR_BYTES);
		} else {
			buf.put(CODE_OK_BYTES);
		}
	}

	@Override
	protected int estimateSize() {
		return estimateSize(LONGEST_FIRST_LINE_SIZE);
	}

	@Override
	protected void writeTo(ByteBuf buf) {
		if (CHECKS) checkState(!isRecycled());
		writeCodeMessage(buf, code);
		writeHeaders(buf);
	}

	@Override
	public String toString() {
		if (isRecycled()) return "{Recycled HttpResponse}";
		return HttpResponse.class.getSimpleName() + ": " + code;
	}
}
