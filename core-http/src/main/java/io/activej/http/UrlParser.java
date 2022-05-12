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
import io.activej.common.ApplicationSettings;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;
import java.util.*;

import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.http.Protocol.*;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("WeakerAccess")
public final class UrlParser {

	public static final byte COLON = ':';
	public static final byte HASH = '#';
	public static final byte SLASH = '/';
	public static final byte QUESTION_MARK = '?';

	private class QueryParamIterator implements Iterator<QueryParameter> {
		private int i = 0;

		@Override
		public boolean hasNext() {
			return i < queryPositions.length && queryPositions[i] != 0;
		}

		@Override
		public @NotNull QueryParameter next() {
			if (!hasNext())
				throw new NoSuchElementException();
			int record = queryPositions[i++];
			int keyStart = record & 0xFFFF;
			int keyEnd = record >>> 16;
			String key = new String(raw, keyStart, keyEnd - keyStart, CHARSET);
			String value = keyValueDecode(raw, keyEnd, limit);
			return new QueryParameter(key, value);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	private static final ThreadLocal<byte[]> CACHED_BUFFERS = new ThreadLocal<>();
	private static final Charset CHARSET = ApplicationSettings.getCharset(UrlParser.class, "charset", ISO_8859_1);

	private static final byte IPV6_OPENING_BRACKET = '[';
	private static final byte[] IPV6_CLOSING_SECTION_WITH_PORT = encodeAscii("]:");
	private static final byte[] PROTOCOL_DELIMITER = encodeAscii("://");

	private final byte[] raw;
	private final short limit;
	private final short offset;

	private int portValue = -1;
	private Protocol protocol;

	private short host = -1;
	private short path = -1;
	private short port = -1;
	private short pathEnd = -1;
	private short query = -1;
	private short fragment = -1;
	short pos = -1;

	int[] queryPositions;

	// region creators
	private UrlParser(byte[] raw, short offset, short limit) {
		this.raw = raw;
		this.offset = offset;
		this.limit = limit;
	}

	public static @NotNull UrlParser of(@NotNull String url) {
		return of(url.getBytes(ISO_8859_1), 0, url.length());
	}

	public static @NotNull UrlParser of(byte[] url, int offset, int limit) {
		try {
			UrlParser httpUrl = createParser(url, offset, limit);
			httpUrl.parse(false);
			return httpUrl;
		} catch (MalformedHttpException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public static @NotNull UrlParser parse(@NotNull String url) throws MalformedHttpException {
		return parse(url.getBytes(ISO_8859_1), 0, url.length());
	}

	public static @NotNull UrlParser parse(byte[] url, int offset, int limit) throws MalformedHttpException {
		UrlParser httpUrl = createParser(url, offset, limit);
		httpUrl.parse(true);
		return httpUrl;
	}
	// endregion

	private static UrlParser createParser(byte[] url, int offset, int limit) throws MalformedHttpException {
		if (limit <= Short.MAX_VALUE) {
			return new UrlParser(url, (short) offset, (short) limit);
		}
		int urlLength = limit - offset;
		if (urlLength > Short.MAX_VALUE) {
			throw new MalformedHttpException("URL length exceeds " + Short.MAX_VALUE + " bytes");
		}
		byte[] urlBytes = new byte[urlLength];
		System.arraycopy(url, offset, urlBytes, 0, urlLength);
		return new UrlParser(urlBytes, (short) 0, (short) urlLength);
	}

	private void parse(boolean isRelativePathAllowed) throws MalformedHttpException {
		int index = indexOf(PROTOCOL_DELIMITER, offset);
		int protocolLength = index - offset;
		if (protocolLength < 0 || protocolLength > 5) {
			if (!isRelativePathAllowed)
				throw new MalformedHttpException("Partial URI is not allowed: " + this);
			index = offset;
		} else {
			if (protocolLength == 5 && startsWith(HTTPS.lowercaseBytes(), offset)) {
				protocol = HTTPS;
			} else if (protocolLength == 4 && startsWith(HTTP.lowercaseBytes(), offset)) {
				protocol = HTTP;
			} else if (protocolLength == 3 && startsWith(WSS.lowercaseBytes(), offset)) {
				protocol = WSS;
			} else if (protocolLength == 2 && startsWith(WS.lowercaseBytes(), offset)) {
				protocol = WS;
			} else {
				throw new MalformedHttpException("Unsupported schema: " + new String(raw, offset, protocolLength, CHARSET));
			}
			index += PROTOCOL_DELIMITER.length;
			host = (short) index;

			int hostPortEnd = findHostPortEnd(host);
			if (host == hostPortEnd || indexOf(COLON, host) == host) {
				throw new MalformedHttpException("Domain name cannot be null or empty");
			}

			if (indexOf(IPV6_OPENING_BRACKET, index) != -1) {                   // parse IPv6
				int closingSection = indexOf(IPV6_CLOSING_SECTION_WITH_PORT, index);
				port = (short) (closingSection != -1 ? (closingSection + 2) : closingSection);
			} else {
				// parse IPv4
				int colon = indexOf(COLON, index);
				port = colon != -1 && colon < hostPortEnd ? (short) (colon + 1) : -1;
			}

			if (has(port)) {
				portValue = parsePort(hostPortEnd);
			} else {
				if (has(host)) {
					portValue = protocol.isSecure() ? 443 : 80;
				}
			}

			index = hostPortEnd;
		}

		if (index == limit) {
			return;
		}

		// parse path
		if (raw[index] == '/') {
			path = (short) index;
			pos = path;
			pathEnd = (short) findPathEnd(path);
			index = pathEnd;
		}

		if (index == limit) {
			return;
		}

		// parse query
		if (raw[index] == '?') {
			query = (short) (index + 1);
			index = findQueryEnd(query);
		}

		if (index == limit) {
			return;
		}

		// parse fragment
		if (raw[index] == '#') {
			fragment = (short) (index + 1);
		}
	}

	private int findHostPortEnd(int from) {
		for (int i = from; i < limit; i++) {
			byte b = raw[i];
			if (b == '/' || b == '?' || b == '#') {
				return i;
			}
		}
		return limit;
	}

	private int findPathEnd(int from) {
		for (int i = from; i < limit; i++) {
			byte b = raw[i];
			if (b == '?' || b == '#') {
				return i;
			}
		}
		return limit;
	}

	private int findQueryEnd(int from) {
		int queryEnd = indexOf(HASH, from);
		return queryEnd != -1 ? queryEnd : limit;
	}

	// getters
	public boolean isRelativePath() {
		return no(host);
	}

	public Protocol getProtocol() {
		return protocol;
	}

	void setProtocol(Protocol protocol) {
		this.protocol = protocol;
	}

	public @Nullable String getHostAndPort() {
		if (no(host)) {
			return null;
		}
		int end = has(path) ? path : has(query) ? query - 1 : has(fragment) ? fragment - 1 : limit;
		return new String(raw, host, end - host, CHARSET);
	}

	public @Nullable String getHost() {
		if (no(host)) {
			return null;
		}
		int end = has(port) ? port - 1 : has(path) ? path : has(query) ? query - 1 : has(fragment) ? fragment - 1 : limit;
		return new String(raw, host, end - host, CHARSET);
	}

	public int getPort() {
		return portValue;
	}

	public @NotNull String getPathAndQuery() {
		if (no(path)) {
			if (no(query))
				return "/";
			else {
				int queryEnd = no(fragment) ? limit : fragment - 1;
				return new String(raw, query, queryEnd - query, CHARSET);
			}
		} else {
			int queryEnd = no(fragment) ? limit : fragment - 1;
			return new String(raw, path, queryEnd - path, CHARSET);
		}
	}

	public @NotNull String getPath() {
		if (no(path)) {
			return "/";
		}
		return new String(raw, path, pathEnd - path, CHARSET);
	}

	public @NotNull String getQuery() {
		if (no(query)) {
			return "";
		}
		int queryEnd = no(fragment) ? limit : fragment - 1;
		return new String(raw, query, queryEnd - query, CHARSET);
	}

	public @NotNull String getFragment() {
		if (no(fragment)) {
			return "";
		}
		return new String(raw, fragment, limit - fragment, CHARSET);
	}

	int getPathAndQueryLength() {
		int len = 0;
		len += no(path) ? 1 : pathEnd - path;
		len += no(query) ? 0 : (no(fragment) ? limit : fragment - 1) - query + 1;
		return len;
	}

	void writePathAndQuery(@NotNull ByteBuf buf) {
		if (no(path)) {
			buf.put(SLASH);
		} else {
			for (int i = path; i < pathEnd; i++) {
				buf.put(raw[i]);
			}
		}
		if (has(query)) {
			buf.put(QUESTION_MARK);
			int queryEnd = no(fragment) ? limit : fragment - 1;
			for (int i = query; i < queryEnd; i++) {
				buf.put(raw[i]);
			}
		}
	}

	// work with parameters
	public @Nullable String getQueryParameter(@NotNull String key) {
		if (no(query)) {
			return null;
		}
		if (queryPositions == null) {
			parseQueryParameters();
		}
		return findParameter(key);
	}

	public @NotNull List<String> getQueryParameters(@NotNull String key) {
		if (no(query)) {
			return List.of();
		}
		if (queryPositions == null) {
			parseQueryParameters();
		}
		return findParameters(key);
	}

	public @NotNull Iterable<QueryParameter> getQueryParametersIterable() {
		if (no(query)) {
			return List.of();
		}
		if (queryPositions == null) {
			parseQueryParameters();
		}
		return QueryParamIterator::new;
	}

	public @NotNull Map<String, String> getQueryParameters() {
		HashMap<String, String> map = new HashMap<>();
		for (QueryParameter queryParameter : getQueryParametersIterable()) {
			map.put(queryParameter.getKey(), queryParameter.getValue());
		}
		return map;
	}

	void parseQueryParameters() {
		int queryEnd = no(fragment) ? limit : fragment - 1;
		queryPositions = parseQueryParameters(queryEnd);
	}

	private static final int[] NO_PARAMETERS = {};

	int[] parseQueryParameters(int end) {
		if (query == end)
			return NO_PARAMETERS;
		assert limit >= end;
		assert has(query);

		int[] positions = new int[8];

		int k = 0;
		int keyStart = query;
		while (keyStart < end) {
			int keyEnd = keyStart;
			while (keyEnd < end) {
				byte b = raw[keyEnd];
				if (b == '&' || b == '=') break;
				keyEnd++;
			}
			if (keyStart != keyEnd) {
				if (k >= positions.length) {
					positions = Arrays.copyOf(positions, positions.length * 2);
				}
				positions[k++] = keyStart | (keyEnd << 16);
			}
			while (keyStart < end) {
				if (raw[keyStart++] == '&') break;
			}
		}

		return positions;
	}

	public static @NotNull Map<String, String> parseQueryIntoMap(@NotNull String query) {
		return parseQueryIntoMap(query.getBytes(ISO_8859_1), 0, query.length());
	}

	static @NotNull Map<String, String> parseQueryIntoMap(byte[] query, int offset, int limit) {
		Map<String, String> result = new LinkedHashMap<>();

		int keyStart = offset;
		while (keyStart < limit) {
			int keyEnd = keyStart;
			while (keyEnd < limit) {
				byte b = query[keyEnd];
				if (b == '&' || b == '=') break;
				keyEnd++;
			}
			if (keyStart != keyEnd) {
				result.putIfAbsent(new String(query, keyStart, keyEnd - keyStart, CHARSET), keyValueDecode(query, keyEnd, limit));
			}
			while (keyStart < limit) {
				if (query[keyStart++] == '&') break;
			}
		}

		return result;
	}

	@Nullable String findParameter(@NotNull String key) {
		for (int record : queryPositions) {
			if (record == 0) break;
			int keyStart = record & 0xFFFF;
			int keyEnd = record >>> 16;
			if (isEqual(key, keyStart, keyEnd)) {
				return keyValueDecode(raw, keyEnd, limit);
			}
		}
		return null;
	}

	@NotNull List<String> findParameters(@NotNull String key) {
		List<String> container = new ArrayList<>();
		for (int record : queryPositions) {
			if (record == 0) break;
			int keyStart = record & 0xFFFF;
			int keyEnd = record >>> 16;
			if (isEqual(key, keyStart, keyEnd)) {
				container.add(keyValueDecode(raw, keyEnd, limit));
			}
		}
		return container;
	}

	// work with path
	@NotNull String getPartialPath() {
		if (no(pos) || pos > pathEnd) {
			return "/";
		}
		return new String(raw, pos, pathEnd - pos, CHARSET);
	}

	@Nullable String pollUrlPart() {
		if (pos < pathEnd) {
			int start = pos + 1;
			int nextSlash = indexOf(SLASH, start);
			pos = nextSlash > pathEnd ? pathEnd : (short) nextSlash;
			if (no(pos)) {
				pos = limit;
				return urlParse(raw, start, pathEnd);
			} else {
				return urlParse(raw, start, pos);
			}
		} else {
			return "";
		}
	}

	private boolean isEqual(@NotNull String key, int start, int end) {
		if (end - start != key.length()) {
			return false;
		}
		for (int i = 0; i < key.length(); i++) {
			if (key.charAt(i) != raw[start + i])
				return false;
		}
		return true;
	}

	private int parsePort(int end) throws MalformedHttpException {
		if (port == end) {
			throw new MalformedHttpException("Empty port value");
		}
		if ((end - port) > 5) {
			throw new MalformedHttpException("Bad port: " + new String(raw, port, end - port, CHARSET));
		}

		int result = 0;
		for (int i = port; i < end; i++) {
			int c = raw[i] - '0';
			if (c < 0 || c > 9)
				throw new MalformedHttpException("Bad port: " + new String(raw, port, end - port, CHARSET));
			result = c + result * 10;
		}

		if (result > 0xFFFF) {
			throw new MalformedHttpException("Bad port: " + new String(raw, port, end - port, CHARSET));
		}

		return result;
	}

	private static @Nullable String keyValueDecode(byte[] url, int keyEnd, int limit) {
		return urlParse(url, keyEnd < limit && url[keyEnd] == '=' ? keyEnd + 1 : keyEnd, limit);
	}

	/**
	 * Parses an application/x-www-form-urlencoded string using a specific encoding scheme. The supplied
	 * encoding is used to determine what characters are represented by any consecutive sequences of the
	 * form "%xy".
	 *
	 * @param s string for decoding
	 * @return the newly parsed String
	 */
	public static @Nullable String urlParse(@NotNull String s) {
		return urlParse(encodeAscii(s), 0, s.length());
	}

	private static @Nullable String urlParse(byte[] url, int pos, int limit) {
		for (int i = pos; i < limit; i++) {
			byte c = url[i];
			if (c == '+' || c == '%')
				return urlParse(url, pos, limit, i); // inline hint
			if (c == '&' || c == '#')
				return new String(url, pos, i - pos, CHARSET);
		}
		return new String(url, pos, limit - pos, CHARSET);
	}

	private static @Nullable String urlParse(byte[] url, int pos, int limit, int encodedSuffixPos) {
		byte[] bytes = CACHED_BUFFERS.get();
		if (bytes == null || bytes.length < limit - pos) {
			int newCount = limit - pos + (limit - pos << 1);
			bytes = new byte[newCount];
			CACHED_BUFFERS.set(bytes);
		}

		int bytesPos = 0;
		for (; pos < encodedSuffixPos; pos++) {
			bytes[bytesPos++] = url[pos];
		}
		try {
			LOOP:
			while (pos < limit) {
				byte b = url[pos];
				switch (b) {
					case '&':
					case '#':
						break LOOP;
					case '+':
						bytes[bytesPos++] = ' ';
						pos++;
						break;
					case '%':
						while ((pos + 2 < limit) && (b == '%')) {
							bytes[bytesPos++] = (byte) ((decodeHex(url[pos + 1]) << 4) + decodeHex(url[pos + 2]));
							pos += 3;
							if (pos < limit) {
								b = url[pos];
							}
						}

						if ((pos < limit) && (b == '%'))
							return null;
						break;
					default:
						bytes[bytesPos++] = b;
						pos++;
						break;
				}
			}
			return new String(bytes, 0, bytesPos, UTF_8);
		} catch (MalformedHttpException e) {
			return null;
		}
	}

	private static byte decodeHex(byte b) throws MalformedHttpException {
		if (b >= '0' && b <= '9') return (byte) (b - '0');
		if (b >= 'a' && b <= 'f') return (byte) (b - 'a' + 10);
		if (b >= 'A' && b <= 'F') return (byte) (b - 'A' + 10);
		throw new MalformedHttpException("Failed to decode hex digit from '" + b + '\'');
	}

	private boolean startsWith(byte[] subArray, int from) {
		for (int j = 0; j < subArray.length; j++) {
			if (subArray[j] != raw[from + j]) {
				return false;
			}
		}
		return true;
	}

	private int indexOf(byte[] subArray, int from) {
		first:
		for (int i = from; i < limit - subArray.length + 1; i++) {
			for (int j = 0; j < subArray.length; j++) {
				if (subArray[j] != raw[i + j]) {
					continue first;
				}
			}
			return i;
		}
		return -1;
	}

	private int indexOf(byte b, int from) {
		for (int i = from; i < limit; i++) {
			if (raw[i] == b) {
				return i;
			}
		}
		return -1;
	}

	private static boolean has(short v) {
		return v >= 0;
	}

	private static boolean no(short v) {
		return v < 0;
	}

	@Override
	public String toString() {
		return new String(raw, offset, limit - offset, CHARSET);
	}
}
