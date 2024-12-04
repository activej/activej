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
import io.activej.http.HttpHeaderValue.HttpHeaderValueOfSetCookies;
import io.activej.promise.Promise;
import io.activej.promise.ToPromise;
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
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Represents an HTTP response for a given {@link HttpRequest}.
 * <p>
 * This class is a final implementation that extends {@link HttpMessage} and implements the {@link ToPromise} interface.
 * Instances of this class are used to create, modify, and send HTTP responses.
 *
 * <p>After handling, {@code HttpResponse} objects are recycled, meaning they cannot be reused.
 * This response object is designed to be constructed through its internal {@link Builder} class,
 * which allows for easy modification and customization of HTTP headers, status codes, and content.
 *
 * <p><strong>Key Features:</strong></p>
 * <ul>
 *     <li>Ability to set and customize HTTP response codes.</li>
 *     <li>Provide a fluent builder API to simplify response creation.</li>
 *     <li>Handle response cookies.</li>
 * </ul>
 *
 * <p>This class supports common HTTP status codes (e.g., 200 OK, 404 Not Found) as well as common response
 * methods for building typical responses (e.g., HTML content, JSON, redirects).
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * HttpResponse response = HttpResponse.ok200()
 *     .withPlainText("Hello, World!")
 *     .build();
 * }</pre>
 *
 * @see HttpRequest
 * @see HttpMessage
 * @see ToPromise
 */
public final class HttpResponse extends HttpMessage implements ToPromise<HttpResponse> {
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

	private int code;

	private @Nullable Map<String, HttpCookie> parsedCookies;

	/**
	 * Constructs a new {@link HttpResponse} with the specified HTTP version and response code.
	 *
	 * @param version The HTTP version (HTTP/1.1).
	 * @param code The HTTP response status code.
	 * @param connection Optional {@link HttpClientConnection} associated with this response.
	 */
	HttpResponse(HttpVersion version, int code, @Nullable HttpClientConnection connection) {
		super(version);
		this.code = code;
		this.connection = connection;
	}

	/**
	 * Constructs a new {@link HttpResponse} with the specified HTTP version and response code,
	 * without an associated connection.
	 *
	 * @param version The HTTP version (HTTP/1.1).
	 * @param code The HTTP response status code.
	 */
	HttpResponse(HttpVersion version, int code) {
		this(version, code, null);
	}

	/**
	 * Creates a new instance of the {@link Builder} for constructing a custom {@link HttpResponse}.
	 *
	 * @return A new {@link Builder} instance.
	 */
	public static Builder builder() {
		return new HttpResponse(HTTP_1_1, 0).new Builder();
	}

	/**
	 * Creates a new {@link Builder} for constructing an {@link HttpResponse} with the specified status code.
	 *
	 * @param code The HTTP status code.
	 * @return A {@link Builder} instance.
	 */
	public static Builder ofCode(int code) {
		return builder().withCode(code);
	}

	/**
	 * Convenience method for creating a response with a 200 OK status code.
	 *
	 * @return A {@link Builder} instance for an HTTP 200 OK response.
	 */
	public static Builder ok200() {
		return ofCode(200);
	}

	/**
	 * Convenience method for creating a response with a 201 Created status code.
	 *
	 * @return A {@link Builder} instance for an HTTP 201 Created response.
	 */
	public static Builder ok201() {
		return ofCode(201);
	}

	public static Builder ok206() {
		return ofCode(206);
	}

	/**
	 * Convenience method for creating a response that redirects to a given URL with a 301 Moved Permanently status code.
	 *
	 * @param url The URL to which the client should be redirected.
	 * @return A {@link Builder} instance for an HTTP 301 Moved Permanently response.
	 */
	public static Builder redirect301(String url) {
		// RFC-7231, section 6.4.2 (https://tools.ietf.org/html/rfc7231#section-6.4.2)
		return ofCode(301)
			.withHeader(LOCATION, url);
	}

	public static Builder redirect302(String url) {
		// RFC-7231, section 6.4.3 (https://tools.ietf.org/html/rfc7231#section-6.4.3)
		return ofCode(302)
			.withHeader(LOCATION, url);
	}

	public static Builder redirect307(String url) {
		return ofCode(307)
			.withHeader(LOCATION, url);
	}

	public static Builder redirect308(String url) {
		// RFC-7238, section 3 (https://tools.ietf.org/html/rfc7238#section-3)
		return ofCode(308)
			.withHeader(LOCATION, url);
	}

	public static Builder unauthorized401(String challenge) {
		// RFC-7235, section 3.1 (https://tools.ietf.org/html/rfc7235#section-3.1)
		return ofCode(401)
			.withHeader(WWW_AUTHENTICATE, challenge);
	}

	public static Builder notFound404() {
		return ofCode(404);
	}

	/**
	 * Builder class for constructing {@link HttpResponse} objects.
	 */
	public final class Builder extends HttpMessage.Builder<Builder, HttpResponse> {
		private Builder() {}

		/**
		 * Sets the HTTP status code for the response.
		 *
		 * @param code The HTTP status code to be set.
		 * @return This {@link Builder} instance for method chaining.
		 */
		public Builder withCode(int code) {
			if (CHECKS) checkArgument(code >= 100 && code < 600, "Code should be in range [100, 600)");
			HttpResponse.this.code = code;
			return this;
		}

		/**
		 * Sets the response body with plain text content.
		 *
		 * @param text The plain text content to be used as the response body.
		 * @return This {@link Builder} instance for method chaining.
		 */
		public Builder withPlainText(String text) {
			return withHeader(CONTENT_TYPE, ofContentType(PLAIN_TEXT_UTF_8))
				.withBody(text.getBytes(UTF_8));
		}

		/**
		 * Sets the response body with HTML content.
		 *
		 * @param text The HTML content to be used as the response body.
		 * @return This {@link Builder} instance for method chaining.
		 */
		public Builder withHtml(String text) {
			return withHeader(CONTENT_TYPE, ofContentType(HTML_UTF_8))
				.withBody(text.getBytes(UTF_8));
		}

		/**
		 * Sets the response body with JSON content.
		 *
		 * @param text The JSON content to be used as the response body.
		 * @return This {@link Builder} instance for method chaining.
		 */
		public Builder withJson(String text) {
			return withHeader(CONTENT_TYPE, ofContentType(JSON_UTF_8))
				.withBody(text.getBytes(UTF_8));
		}

		/**
		 * Adds a list of cookies to the response.
		 *
		 * @param cookies The list of {@link HttpCookie} objects to be added to the response.
		 */
		@Override
		protected void addCookies(List<HttpCookie> cookies) {
			for (HttpCookie cookie : cookies) {
				addCookie(cookie);
			}
		}

		/**
		 * Adds a single cookie to the response.
		 *
		 * @param cookie The {@link HttpCookie} object to be added to the response.
		 */
		@Override
		protected void addCookie(HttpCookie cookie) {
			headers.add(SET_COOKIE, new HttpHeaderValueOfSetCookies(cookie));
		}

		/**
		 * Builds and returns the configured {@link HttpResponse} object.
		 *
		 * @return The constructed {@link HttpResponse} object.
		 * @throws IllegalArgumentException if the response code has not been set.
		 */
		@Override
		public HttpResponse build() {
			HttpResponse httpResponse = super.build();
			checkArgument(httpResponse.code > 0);
			return httpResponse;
		}
	}

	/**
	 * Converts the {@link HttpResponse} instance to a {@link Promise} that is resolved with this response.
	 *
	 * @return A {@link Promise} containing the {@link HttpResponse} instance.
	 */
	@Override
	public Promise<HttpResponse> toPromise() {
		return Promise.of(this);
	}

	@Override
	boolean isContentLengthExpected() {
		return true;
	}

	/**
	 * Retrieves the connection associated with this response.
	 *
	 * @return The {@link HttpClientConnection} associated with this response, or {@code null} if none.
	 */
	public HttpClientConnection getConnection() {
		return connection;
	}

	/**
	 * Retrieves the HTTP response status code.
	 *
	 * @return The HTTP response status code.
	 */
	public int getCode() {
		return code;
	}

	/**
	 * Retrieves the cookies present in this response.
	 * If the response has been recycled, an exception will be thrown.
	 *
	 * @return A map containing the cookie names and their corresponding {@link HttpCookie} objects.
	 */
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

	/**
	 * Retrieves a specific cookie by its name.
	 *
	 * @param cookie The name of the cookie to be retrieved.
	 * @return The {@link HttpCookie} object representing the cookie, or {@code null} if not found.
	 */
	public @Nullable HttpCookie getCookie(String cookie) {
		if (CHECKS) checkState(!isRecycled());
		return getCookies().get(cookie);
	}

	/**
	 * Writes the corresponding HTTP response code message to the specified {@link ByteBuf} buffer.
	 *
	 * <p>This method uses a {@code switch} statement to map the provided status code to its corresponding
	 * predefined byte array representation. If the code matches a standard HTTP status code (e.g., 200 OK, 404 Not Found),
	 * the corresponding pre-encoded byte array is written to the buffer. If the status code is not recognized
	 * in the predefined set, it delegates the task to {@link #writeCodeMessage2(ByteBuf, int)}.
	 *
	 * <p>If the code is not covered in the predefined cases, the method calls {@code writeCodeMessage2} to handle
	 * the response in a more generic way.
	 *
	 * @param buf The {@link ByteBuf} to which the response line should be written.
	 * @param code The HTTP status code to be written.
	 */
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

	/**
	 * Writes an HTTP response code message to the given {@link ByteBuf} buffer.
	 *
	 * <p>This method is used to construct an HTTP response line starting with "HTTP/1.1", followed by
	 * the response code, and appending either " OK" or " Error" based on the status code.
	 *
	 * <p>If the response code is 400 or greater, the phrase " Error" is appended, otherwise the phrase " OK" is appended.
	 * This is typically used for handling non-standard HTTP codes.
	 *
	 * @param buf The {@link ByteBuf} to which the response line should be written.
	 * @param code The HTTP status code to be written.
	 */
	private static void writeCodeMessage2(ByteBuf buf, int code) {
		buf.put(HTTP11_BYTES);
		putPositiveInt(buf, code);
		if (code >= 400) {
			buf.put(CODE_ERROR_BYTES);
		} else {
			buf.put(CODE_OK_BYTES);
		}
	}

	/**
	 * Estimates the size of the response in bytes, considering headers and the response line.
	 *
	 * <p>This method is used internally to estimate the required size for the {@link ByteBuf} that stores
	 * the response content. It uses {@code LONGEST_FIRST_LINE_SIZE} as the basis for calculating the response size.
	 *
	 * @return The estimated size of the HTTP response.
	 */
	@Override
	protected int estimateSize() {
		return estimateSize(LONGEST_FIRST_LINE_SIZE);
	}

	/**
	 * Writes the entire HTTP response to the specified {@link ByteBuf}.
	 *
	 * <p>This method writes the response code message, followed by all HTTP headers.
	 *
	 * @param buf The {@link ByteBuf} buffer to which the response data should be written.
	 * @throws IllegalStateException if the response has already been recycled.
	 */
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
