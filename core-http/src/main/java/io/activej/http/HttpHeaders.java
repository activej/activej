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

import static io.activej.bytebuf.ByteBufStrings.encodeAscii;

/**
 * This is a collection of most well-known {@link HttpHeader} token references as Java constants.
 */
public final class HttpHeaders {
	private static final CaseInsensitiveTokenMap<HttpHeader> headers = new CaseInsensitiveTokenMap<>(512, 2, HttpHeader.class, HttpHeader::new);

	public static final HttpHeader CACHE_CONTROL = headers.register("Cache-Control");
	public static final HttpHeader CONTENT_LENGTH = headers.register("Content-Length");
	public static final HttpHeader CONTENT_TYPE = headers.register("Content-Type");
	public static final HttpHeader DATE = headers.register("Date");
	public static final HttpHeader PRAGMA = headers.register("Pragma");
	public static final HttpHeader VIA = headers.register("Via");
	public static final HttpHeader WARNING = headers.register("Warning");
	public static final HttpHeader ACCEPT = headers.register("Accept");
	public static final HttpHeader ACCEPT_CHARSET = headers.register("Accept-Charset");
	public static final HttpHeader ACCEPT_ENCODING = headers.register("Accept-Encoding");
	public static final HttpHeader ACCEPT_LANGUAGE = headers.register("Accept-Language");
	public static final HttpHeader ACCESS_CONTROL_REQUEST_HEADERS = headers.register("Access-Control-Request-Headers");
	public static final HttpHeader ACCESS_CONTROL_REQUEST_METHOD = headers.register("Access-Control-Request-Method");
	public static final HttpHeader AUTHORIZATION = headers.register("Authorization");
	public static final HttpHeader CONNECTION = headers.register("Connection");
	public static final HttpHeader COOKIE = headers.register("Cookie");
	public static final HttpHeader EXPECT = headers.register("Expect");
	public static final HttpHeader FROM = headers.register("From");
	public static final HttpHeader FOLLOW_ONLY_WHEN_PRERENDER_SHOWN = headers.register("Follow-Only-When-Prerender-Shown");
	public static final HttpHeader HOST = headers.register("Host");
	public static final HttpHeader IF_MATCH = headers.register("If-Match");
	public static final HttpHeader IF_MODIFIED_SINCE = headers.register("If-Modified-Since");
	public static final HttpHeader IF_NONE_MATCH = headers.register("If-None-Match");
	public static final HttpHeader IF_RANGE = headers.register("If-Range");
	public static final HttpHeader IF_UNMODIFIED_SINCE = headers.register("If-Unmodified-Since");
	public static final HttpHeader LAST_EVENT_ID = headers.register("Last-Event-ID");
	public static final HttpHeader MAX_FORWARDS = headers.register("Max-Forwards");
	public static final HttpHeader ORIGIN = headers.register("Origin");
	public static final HttpHeader PROXY_AUTHORIZATION = headers.register("Proxy-Authorization");
	public static final HttpHeader RANGE = headers.register("Range");
	public static final HttpHeader REFERER = headers.register("Referer");
	public static final HttpHeader TE = headers.register("TE");
	public static final HttpHeader UPGRADE = headers.register("Upgrade");
	public static final HttpHeader USER_AGENT = headers.register("User-Agent");
	public static final HttpHeader ACCEPT_RANGES = headers.register("Accept-Ranges");
	public static final HttpHeader ACCESS_CONTROL_ALLOW_HEADERS = headers.register("Access-Control-Allow-Headers");
	public static final HttpHeader ACCESS_CONTROL_ALLOW_METHODS = headers.register("Access-Control-Allow-Methods");
	public static final HttpHeader ACCESS_CONTROL_ALLOW_ORIGIN = headers.register("Access-Control-Allow-Origin");
	public static final HttpHeader ACCESS_CONTROL_ALLOW_CREDENTIALS = headers.register("Access-Control-Allow-Credentials");
	public static final HttpHeader ACCESS_CONTROL_EXPOSE_HEADERS = headers.register("Access-Control-Expose-Headers");
	public static final HttpHeader ACCESS_CONTROL_MAX_AGE = headers.register("Access-Control-Max-Age");
	public static final HttpHeader AGE = headers.register("Age");
	public static final HttpHeader ALLOW = headers.register("Allow");
	public static final HttpHeader CONTENT_DISPOSITION = headers.register("Content-Disposition");
	public static final HttpHeader CONTENT_ENCODING = headers.register("Content-Encoding");
	public static final HttpHeader CONTENT_LANGUAGE = headers.register("Content-Language");
	public static final HttpHeader CONTENT_LOCATION = headers.register("Content-Location");
	public static final HttpHeader CONTENT_MD5 = headers.register("Content-MD5");
	public static final HttpHeader CONTENT_RANGE = headers.register("Content-Range");
	public static final HttpHeader CONTENT_SECURITY_POLICY = headers.register("Content-Security-Policy");
	public static final HttpHeader CONTENT_SECURITY_POLICY_REPORT_ONLY = headers.register("Content-Security-Policy-Report-Only");
	public static final HttpHeader ETAG = headers.register("ETag");
	public static final HttpHeader EXPIRES = headers.register("Expires");
	public static final HttpHeader LAST_MODIFIED = headers.register("Last-Modified");
	public static final HttpHeader LINK = headers.register("Link");
	public static final HttpHeader LOCATION = headers.register("Location");
	public static final HttpHeader P3P = headers.register("P3P");
	public static final HttpHeader PROXY_AUTHENTICATE = headers.register("Proxy-Authenticate");
	public static final HttpHeader REFRESH = headers.register("Refresh");
	public static final HttpHeader RETRY_AFTER = headers.register("Retry-After");
	public static final HttpHeader SERVER = headers.register("Server");
	public static final HttpHeader SET_COOKIE = headers.register("Set-Cookie");
	public static final HttpHeader STRICT_TRANSPORT_SECURITY = headers.register("Strict-Transport-Security");
	public static final HttpHeader TIMING_ALLOW_ORIGIN = headers.register("Timing-Allow-Origin");
	public static final HttpHeader TRAILER = headers.register("Trailer");
	public static final HttpHeader TRANSFER_ENCODING = headers.register("Transfer-Encoding");
	public static final HttpHeader VARY = headers.register("Vary");
	public static final HttpHeader WWW_AUTHENTICATE = headers.register("WWW-Authenticate");
	public static final HttpHeader DNT = headers.register("DNT");
	public static final HttpHeader X_CONTENT_TYPE_OPTIONS = headers.register("X-Content-Type-Options");
	public static final HttpHeader X_DO_NOT_TRACK = headers.register("X-Do-Not-Track");
	public static final HttpHeader X_FORWARDED_FOR = headers.register("X-Forwarded-For");
	public static final HttpHeader X_FORWARDED_PROTO = headers.register("X-Forwarded-Proto");
	public static final HttpHeader X_FRAME_OPTIONS = headers.register("X-Frame-Options");
	public static final HttpHeader X_POWERED_BY = headers.register("X-Powered-By");
	public static final HttpHeader PUBLIC_KEY_PINS = headers.register("Public-Key-Pins");
	public static final HttpHeader PUBLIC_KEY_PINS_REPORT_ONLY = headers.register("Public-Key-Pins-Report-Only");
	public static final HttpHeader X_REQUESTED_WITH = headers.register("X-Requested-With");
	public static final HttpHeader X_USER_IP = headers.register("X-User-IP");
	public static final HttpHeader X_XSS_PROTECTION = headers.register("X-XSS-Protection");

	public static final HttpHeader X_REAL_IP = headers.register("X-Real-IP");
	public static final HttpHeader X_AUTH_TOKEN = headers.register("X-Auth-Token");

	public static final HttpHeader SEC_WEBSOCKET_KEY = headers.register("Sec-WebSocket-Key");
	public static final HttpHeader SEC_WEBSOCKET_ACCEPT = headers.register("Sec-WebSocket-Accept");
	public static final HttpHeader SEC_WEBSOCKET_VERSION = headers.register("Sec-WebSocket-Version");

	public static HttpHeader of(byte[] array, int offset, int length, int lowerCaseHashCode) {
		return headers.getOrCreate(array, offset, length, lowerCaseHashCode);
	}

	public static HttpHeader of(String string) {
		byte[] array = encodeAscii(string);
		return headers.getOrCreate(array, 0, array.length);
	}

}
