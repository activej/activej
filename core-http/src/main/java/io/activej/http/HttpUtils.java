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
import io.activej.common.exception.MalformedDataException;
import io.activej.http.IWebSocket.Frame.FrameType;
import io.activej.http.IWebSocket.Message.MessageType;
import io.activej.http.WebSocketConstants.OpCode;
import io.activej.net.AbstractReactiveServer;
import org.jetbrains.annotations.Nullable;

import java.io.UnsupportedEncodingException;
import java.net.*;
import java.nio.charset.CharacterCodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.http.HttpHeaders.SEC_WEBSOCKET_ACCEPT;
import static io.activej.http.IWebSocket.Frame.FrameType.*;
import static io.activej.http.WebSocketConstants.MAGIC_STRING;
import static io.activej.http.WebSocketConstants.OpCode.*;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

/**
 * Util for working with {@link HttpRequest}
 */
public final class HttpUtils {
	private static final int URI_DEFAULT_CAPACITY = 1 << 5;
	private static final int LIMIT_INT = Integer.MAX_VALUE / 10;
	private static final int REMAINDER_INT = Integer.MAX_VALUE - LIMIT_INT * 10;
	private static final long LIMIT_LONG = Long.MAX_VALUE / 10;
	private static final long REMAINDER_LONG = Long.MAX_VALUE - LIMIT_LONG * 10;

	static final byte EQUALS = '=';
	static final byte SEMICOLON = ';';
	static final byte COLON = ':';
	static final byte Q = 'q';
	static final byte DOT = '.';
	static final byte ZERO = '0';
	static final byte COMMA = ',';

	public static InetAddress inetAddress(String host) {
		try {
			return InetAddress.getByName(host);
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException(e);
		}
	}

	// https://url.spec.whatwg.org/
	public static boolean isInetAddress(String host) {
		int colons = 0;
		int dots = 0;
		byte[] bytes = encodeAscii(host);

		// expect ipv6 address
		if (bytes[0] == '[') {
			return bytes[bytes.length - 1] == ']' && checkIpv6(bytes, 1, bytes.length - 1);
		}

		// assume ipv4 could be as oct, bin, dec; ipv6 - hex
		for (byte b : bytes) {
			if (b == '.') {
				dots++;
			} else if (b == ':') {
				if (dots != 0) {
					return false;
				}
				colons++;
			} else if (Character.digit(b, 16) == -1) {
				return false;
			}
		}

		if (dots < 4) {
			if (colons > 0 && colons < 8) {
				return checkIpv6(bytes, 0, bytes.length);
			}
			return checkIpv4(bytes, 0, bytes.length);
		}
		return false;
	}

	/*
	 * Checks only for a dot decimal format (192.168.0.208 for example) more -> https://en.wikipedia.org/wiki/IPv4
	 */
	private static boolean checkIpv4(byte[] bytes, int pos, int length) {
		int start = pos;
		for (int i = pos; i < length; i++) {
			// assume at least one more symbol is present after dot
			if (i == length - 1 && bytes[i] == '.') {
				return false;
			}
			if (bytes[i] == '.' || i == length - 1) {
				int v;
				if (i - start == 0 && i != length - 1) {
					return false;
				}
				try {
					v = trimAndDecodePositiveInt(bytes, start, i - start);
				} catch (MalformedHttpException e) {
					return false;
				}
				if (v < 0 || v > 255) return false;
				start = i + 1;
			}
		}
		return true;
	}

	/*
	 *   http://stackoverflow.com/questions/5963199/ipv6-validation
	 *   rfc4291
	 *
	 *   IPV6 addresses are represented as 8, 4 hex digit groups of numbers
	 *   2001:0db8:11a3:09d7:1f34:8a2e:07a0:765d
	 *
	 *   leading zeros are not necessary, however at least one digit should be present
	 *
	 *   the null group ':0000:0000:0000'(one or more) could be substituted with '::' once per address
	 *
	 *   x:x:x:x:x:x:d.d.d.d - 6 ipv6 + 4 ipv4
	 *   ::d.d.d.d
	 * */
	private static boolean checkIpv6(byte[] bytes, int pos, int length) {
		boolean shortHand = false;  // denotes usage of ::
		int numCount = 0;
		int blocksCount = 0;
		int start = 0;
		while (pos < length) {
			if (bytes[pos] == ':') {
				start = pos;
				blocksCount++;
				numCount = 0;
				if (pos > 0 && bytes[pos - 1] == ':') {
					if (shortHand) return false;
					else {
						shortHand = true;
					}
				}
			} else if (bytes[pos] == '.') {
				return checkIpv4(bytes, start + 1, length - start + 1);
			} else {
				if (Character.digit(bytes[pos], 16) == -1) {
					return false;
				}
				numCount++;
				if (numCount > 4) {
					return false;
				}
			}
			pos++;
		}
		return blocksCount > 6 || shortHand;
	}

	public static int skipSpaces(byte[] bytes, int pos, int end) {
		while (pos < end && bytes[pos] == ' ') {
			pos++;
		}
		return pos;
	}

	public static int decodeQ(byte[] bytes, int pos, int length) throws MalformedHttpException {
		if (length == 0) {
			return 100;
		}
		if (bytes[pos] == '1') {
			return 100;
		} else if (bytes[pos] == '0') {
			if (length == 1) return 0;
			length = length > 4 ? 2 : length - 2;
			int q = trimAndDecodePositiveInt(bytes, pos + 2, length);
			if (length == 1) q *= 10;
			return q;
		}
		throw new MalformedHttpException("Value of 'q' should start either from 0 or 1");
	}

	/**
	 * Method which creates string with parameters and its value in format URL. Using encoding UTF-8
	 *
	 * @param q map in which keys if name of parameters, value - value of parameters.
	 * @return string with parameters and its value in format URL
	 */
	public static String renderQueryString(Map<String, String> q) {
		return renderQueryString(q, "UTF-8");
	}

	/**
	 * Method which creates string with parameters and its value in format URL
	 *
	 * @param q   map in which keys if name of parameters, value - value of parameters.
	 * @param enc encoding of this string
	 * @return string with parameters and its value in format URL
	 */
	public static String renderQueryString(Map<String, String> q, String enc) {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, String> e : q.entrySet()) {
			String name = urlEncode(e.getKey(), enc);
			sb.append(name);
			if (e.getValue() != null) {
				sb.append('=');
				sb.append(urlEncode(e.getValue(), enc));
			}
			sb.append('&');
		}
		if (sb.length() > 0)
			sb.setLength(sb.length() - 1);
		return sb.toString();
	}

	/**
	 * Translates a string into application/x-www-form-urlencoded format using a specific encoding scheme.
	 * This method uses the supplied encoding scheme to obtain the bytes for unsafe characters
	 *
	 * @param string string for encoding
	 * @param enc    new encoding
	 * @return the translated String.
	 */
	public static String urlEncode(String string, String enc) {
		try {
			return URLEncoder.encode(string, enc);
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("Can't encode with supplied encoding: " + enc, e);
		}
	}

	public static String urlDecode(@Nullable String string, String enc) throws MalformedHttpException {
		if (string == null) {
			throw new MalformedHttpException("No string to decode");
		}
		try {
			return URLDecoder.decode(string, enc);
		} catch (UnsupportedEncodingException e) {
			throw new MalformedHttpException("Can't encode with supplied encoding: " + enc, e);
		}
	}

	public static int trimAndDecodePositiveInt(byte[] array, int pos, int len) throws MalformedHttpException {
		int left = trimLeft(array, pos, len);
		pos += left;
		len -= left;
		len -= trimRight(array, pos, len);
		return decodePositiveInt(array, pos, len);
	}

	public static long trimAndDecodePositiveLong(byte[] array, int pos, int len) throws MalformedHttpException {
		int left = trimLeft(array, pos, len);
		pos += left;
		len -= left;
		len -= trimRight(array, pos, len);
		return decodePositiveLong(array, pos, len);
	}

	private static int trimLeft(byte[] array, int pos, int len) {
		for (int i = 0; i < len; i++) {
			if (array[pos + i] != SP && array[pos + i] != HT) {
				return i;
			}
		}
		return 0;
	}

	private static int trimRight(byte[] array, int pos, int len) {
		for (int i = len - 1; i >= 0; i--) {
			if (array[pos + i] != SP && array[pos + i] != HT) {
				return len - i - 1;
			}
		}
		return 0;
	}

	/**
	 * (RFC3986) scheme://authority/path/?query#fragment
	 */
	public static @Nullable String getFullUri(HttpRequest request, int builderCapacity) {
		String host = request.getHostAndPort();
		if (host == null) {
			return null;
		}
		String query = request.getQuery();
		String fragment = request.getFragment();
		StringBuilder fullUriBuilder = new StringBuilder(builderCapacity)
			.append(request.getProtocol().lowercase())
			.append("://")
			.append(host)
			.append(request.getPath());
		if (!query.isEmpty()) {
			fullUriBuilder.append("?").append(query);
		}
		if (!fragment.isEmpty()) {
			fullUriBuilder.append("#").append(fragment);
		}
		return fullUriBuilder.toString();
	}

	public static @Nullable String getFullUri(HttpRequest request) {
		return getFullUri(request, URI_DEFAULT_CAPACITY);
	}

	/**
	 * RFC-7231, sections 6.5 and 6.6
	 */
	public static String getHttpErrorTitle(int code) {
		return switch (code) {
			case 400 -> "400. Bad Request";
			case 402 -> "402. Payment Required";
			case 403 -> "403. Forbidden";
			case 404 -> "404. Not Found";
			case 405 -> "405. Method Not Allowed";
			case 406 -> "406. Not Acceptable";
			case 408 -> "408. Request Timeout";
			case 409 -> "409. Conflict";
			case 410 -> "410. Gone";
			case 411 -> "411. Length Required";
			case 413 -> "413. Payload Too Large";
			case 414 -> "414. URI Too Long";
			case 415 -> "415. Unsupported Media Type";
			case 417 -> "417. Expectation Failed";
			case 426 -> "426. Upgrade Required";
			case 500 -> "500. Internal Server Error";
			case 501 -> "501. Not Implemented";
			case 502 -> "502. Bad Gateway";
			case 503 -> "503. Service Unavailable";
			case 504 -> "504. Gateway Timeout";
			case 505 -> "505. HTTP Version Not Supported";
			default -> code + ". Unknown HTTP code, returned from an error";
		};
	}

	/**
	 * Format an IPv4/IPv6 socket address as either HTTP or HTTPS URL.
	 *
	 * @return the URL
	 */
	public static String formatUrl(InetSocketAddress address, boolean ssl) {
		return
			(ssl ? "https://" : "http://") +
			formatHost(address.getAddress()) +
			(address.getPort() != (ssl ? 443 : 80) ? ":" + address.getPort() : "") +
			"/";
	}

	public static List<String> getHttpAddresses(AbstractReactiveServer server) {
		return Stream.concat(
				server.getBoundAddresses().stream().map(address -> HttpUtils.formatUrl(address, false)),
				server.getSslBoundAddresses().stream().map(address -> HttpUtils.formatUrl(address, true))
			)
			.collect(toList());
	}

	private static String formatHost(InetAddress address) {
		String name = address.getHostName();
		if (address instanceof Inet4Address) {
			return name;
		} else if (address instanceof Inet6Address && name.contains(":")) {
			return "[" + name + "]";
		} else {
			return name;
		}
	}

	static String getWebSocketAnswer(String key) {
		String answer;
		try {
			answer = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-1")
				.digest((key + MAGIC_STRING).getBytes(UTF_8)));
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
		return answer;
	}

	static byte[] generateWebSocketKey() {
		byte[] key = new byte[16];
		ThreadLocalRandom.current().nextBytes(key);
		return Base64.getEncoder().encode(key);
	}

	static boolean isAnswerInvalid(HttpResponse response, byte[] key) {
		String header = response.getHeader(SEC_WEBSOCKET_ACCEPT);
		return header == null || !getWebSocketAnswer(new String(key, ISO_8859_1)).equals(header.trim());
	}

	static boolean isReservedCloseCode(int closeCode) {
		return
			closeCode < 1000 ||
			(closeCode >= 1004 && closeCode < 1007) ||
			(closeCode >= 1015 && closeCode < 3000);
	}

	static String getUTF8(ByteBuf buf) throws CharacterCodingException {
		return UTF_8.newDecoder().decode(buf.toReadByteBuffer()).toString();
	}

	static OpCode frameToOpType(FrameType frameType) {
		return switch (frameType) {
			case TEXT -> OP_TEXT;
			case BINARY -> OP_BINARY;
			case CONTINUATION -> OP_CONTINUATION;
		};
	}

	static FrameType opToFrameType(OpCode opType) {
		return switch (opType) {
			case OP_TEXT -> TEXT;
			case OP_BINARY -> BINARY;
			case OP_CONTINUATION -> CONTINUATION;
			default -> throw new AssertionError();
		};
	}

	static MessageType frameToMessageType(FrameType frameType) {
		return switch (frameType) {
			case TEXT -> MessageType.TEXT;
			case BINARY -> MessageType.BINARY;
			default -> throw new AssertionError();
		};
	}

	static Exception translateToHttpException(Exception e) {
		if (e instanceof HttpException) return e;
		if (e instanceof MalformedDataException) return new MalformedHttpException(e);
		return new HttpException(e);
	}

	static int decodePositiveInt(byte[] array, int pos, int len) throws MalformedHttpException {
		int result = 0;
		for (int i = pos; i < pos + len; i++) {
			byte b = (byte) (array[i] - '0');
			if (b < 0 || b >= 10) {
				throw new MalformedHttpException("Not a decimal value: " + new String(array, pos, len, ISO_8859_1));
			}
			if (result >= LIMIT_INT) {
				if (result != LIMIT_INT || b > REMAINDER_INT) {
					throw new MalformedHttpException("Bigger than max int value: " + new String(array, pos, len, ISO_8859_1));
				}
			}
			result = b + result * 10;
		}
		return result;
	}

	static long decodePositiveLong(byte[] array, int pos, int len) throws MalformedHttpException {
		long result = 0;
		for (int i = pos; i < pos + len; i++) {
			byte b = (byte) (array[i] - '0');
			if (b < 0 || b >= 10) {
				throw new MalformedHttpException("Not a decimal value: " + new String(array, pos, len, ISO_8859_1));
			}
			if (result >= LIMIT_LONG) {
				if (result != LIMIT_LONG || b > REMAINDER_LONG) {
					throw new MalformedHttpException("Bigger than max long value: " + new String(array, pos, len, ISO_8859_1));
				}
			}
			result = b + result * 10;
		}
		return result;
	}

	static int hashCodeCI(byte[] array, int offset, int size) {
		int result = 0;
		for (int i = offset; i < offset + size; i++) {
			byte b = array[i];
			result += (b | 0x20);
		}
		return result;
	}

	static int hashCodeCI(byte[] array) {
		return hashCodeCI(array, 0, array.length);
	}

	static void tryAddHeader(HttpMessage httpMessage, HttpHeader header, Supplier<HttpHeaderValue> headerValueSupplier) {
		HttpHeaderValue existing = httpMessage.headers.get(header);
		if (existing != null) return;

		httpMessage.headers.add(header, headerValueSupplier.get());
	}
}
