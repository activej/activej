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
import io.activej.bytebuf.ByteBufStrings;
import io.activej.common.exception.parse.InvalidSizeException;
import io.activej.common.exception.parse.ParseException;
import io.activej.common.exception.parse.UnknownFormatException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static io.activej.http.Protocol.*;
import static java.util.Collections.emptyList;

@SuppressWarnings("WeakerAccess")
public final class UrlParser {
	private static class QueryParamIterator implements Iterator<QueryParameter> {
		private final String src;
		private final int[] positions;
		private int i = 0;

		private QueryParamIterator(String src, int[] positions) {
			this.src = src;
			this.positions = positions;
		}

		@Override
		public boolean hasNext() {
			return i < positions.length && positions[i] != 0;
		}

		@NotNull
		@Override
		public QueryParameter next() {
			if (!hasNext())
				throw new NoSuchElementException();
			int record = positions[i++];
			int keyStart = record & 0xFFFF;
			int keyEnd = record >>> 16;
			String key = src.substring(keyStart, keyEnd);
			String value = keyValueDecode(src, keyEnd);
			return new QueryParameter(key, value);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	private static final class CachedBuffers {
		private final char[] chars;
		private final byte[] bytes;

		private CachedBuffers(char[] chars, byte[] bytes) {
			this.chars = chars;
			this.bytes = bytes;
		}
	}

	private static final ThreadLocal<CachedBuffers> CACHED_BUFFERS = new ThreadLocal<>();

	private static final char IPV6_OPENING_BRACKET = '[';
	private static final String IPV6_CLOSING_SECTION_WITH_PORT = "]:";

	private final String raw;

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
	private UrlParser(String raw) {
		this.raw = raw;
	}

	@NotNull
	public static UrlParser of(@NotNull String url) {
		UrlParser httpUrl = new UrlParser(url);
		try {
			httpUrl.parse(false);
		} catch (ParseException e) {
			throw new IllegalArgumentException(e);
		}
		return httpUrl;
	}

	@NotNull
	public static UrlParser parse(@NotNull String url) throws ParseException {
		UrlParser httpUrl = new UrlParser(url);
		httpUrl.parse(true);
		return httpUrl;
	}
	// endregion

	private void parse(boolean isRelativePathAllowed) throws ParseException {
		if (raw.length() > Short.MAX_VALUE) {
			throw new InvalidSizeException(UrlParser.class, "HttpUrl length cannot be greater than " + Short.MAX_VALUE);
		}

		short index = (short) raw.indexOf("://");
		if (index < 0 || index > 5) {
			if (!isRelativePathAllowed)
				throw new ParseException(UrlParser.class, "Partial URI is not allowed: " + raw);
			index = 0;
		} else {
			if (index == 5 && raw.startsWith(HTTPS.lowercase())) {
				protocol = HTTPS;
			} else if (index == 4 && raw.startsWith(HTTP.lowercase())) {
				protocol = HTTP;
			} else if (index == 3 && raw.startsWith(WSS.lowercase())) {
				protocol = WSS;
			} else if (index == 2 && raw.startsWith(WS.lowercase())) {
				protocol = WS;
			} else {
				throw new UnknownFormatException(UrlParser.class, "Unsupported schema: " + raw.substring(0, index));
			}
			index += "://".length();
			host = index;

			short hostPortEnd = findHostPortEnd(host);
			if (host == hostPortEnd || raw.indexOf(':', host) == host) {
				throw new ParseException("Domain name cannot be null or empty");
			}

			if (raw.indexOf(IPV6_OPENING_BRACKET, index) != -1) {                   // parse IPv6
				int closingSection = raw.indexOf(IPV6_CLOSING_SECTION_WITH_PORT, index);
				port = (short) (closingSection != -1 ? closingSection + 2 : closingSection);
			} else {
				// parse IPv4
				int colon = raw.indexOf(':', index);
				port = (short) ((colon != -1 && colon < hostPortEnd) ? colon + 1 : -1);
			}

			if (port != -1) {
				portValue = toInt(raw, port, hostPortEnd);
			} else {
				if (host != -1) {
					portValue = protocol.isSecure() ? 443 : 80;
				}
			}

			index = hostPortEnd;
		}

		if (index == raw.length()) {
			return;
		}

		// parse path
		if (raw.charAt(index) == '/') {
			path = index;
			pos = path;
			pathEnd = findPathEnd(path);
			index = pathEnd;
		}

		if (index == raw.length()) {
			return;
		}

		// parse query
		if (raw.charAt(index) == '?') {
			query = (short) (index + 1);
			index = findQueryEnd(query);
		}

		if (index == raw.length()) {
			return;
		}

		// parse fragment
		if (raw.charAt(index) == '#') {
			fragment = (short) (index + 1);
		}
	}

	private short findHostPortEnd(short from) {
		short hostPortEnd = -1;
		for (short i = from; i < raw.length(); i++) {
			char ch = raw.charAt(i);
			if (ch == '/' || ch == '?' || ch == '#') {
				hostPortEnd = i;
				break;
			}
		}
		return hostPortEnd != -1 ? hostPortEnd : (short) raw.length();
	}

	private short findPathEnd(short from) {
		short pathEnd = -1;
		for (short i = from; i < raw.length(); i++) {
			char ch = raw.charAt(i);
			if (ch == '?' || ch == '#') {
				pathEnd = i;
				break;
			}
		}
		return pathEnd != -1 ? pathEnd : (short) raw.length();
	}

	private short findQueryEnd(short from) {
		short queryEnd = (short) raw.indexOf('#', from);
		return queryEnd != -1 ? queryEnd : (short) raw.length();
	}

	// getters
	public boolean isRelativePath() {
		return host == -1;
	}

	public Protocol getProtocol() {
		return protocol;
	}

	void setProtocol(Protocol protocol) {
		this.protocol = protocol;
	}

	@Nullable
	public String getHostAndPort() {
		if (host == -1) {
			return null;
		}
		int end = path != -1 ? path : query != -1 ? query - 1 : fragment != -1 ? fragment - 1 : raw.length();
		return raw.substring(host, end);
	}

	@Nullable
	public String getHost() {
		if (host == -1) {
			return null;
		}
		int end = port != -1 ? port - 1 : path != -1 ? path : query != -1 ? query - 1 : raw.length();
		return raw.substring(host, end);
	}

	public int getPort() {
		return portValue;
	}

	@NotNull
	public String getPathAndQuery() {
		if (path == -1) {
			if (query == -1)
				return "/";
			else {
				int queryEnd = fragment == -1 ? raw.length() : fragment - 1;
				return raw.substring(query, queryEnd);
			}
		} else {
			int queryEnd = fragment == -1 ? raw.length() : fragment - 1;
			return raw.substring(path, queryEnd);
		}
	}

	@NotNull
	public String getPath() {
		if (path == -1) {
			return "/";
		}
		return raw.substring(path, pathEnd);
	}

	@NotNull
	public String getQuery() {
		if (query == -1) {
			return "";
		}
		int queryEnd = fragment == -1 ? raw.length() : fragment - 1;
		return raw.substring(query, queryEnd);
	}

	@NotNull
	public String getFragment() {
		if (fragment == -1) {
			return "";
		}
		return raw.substring(fragment);
	}

	int getPathAndQueryLength() {
		int len = 0;
		len += path == -1 ? 1 : pathEnd - path;
		len += query == -1 ? 0 : (fragment == -1 ? raw.length() : fragment - 1) - query + 1;
		return len;
	}

	void writePathAndQuery(@NotNull ByteBuf buf) {
		if (path == -1) {
			buf.put((byte) '/');
		} else {
			for (int i = path; i < pathEnd; i++) {
				buf.put((byte) raw.charAt(i));
			}
		}
		if (query != -1) {
			buf.put((byte) '?');
			int queryEnd = fragment == -1 ? raw.length() : fragment - 1;
			for (int i = query; i < queryEnd; i++) {
				buf.put((byte) raw.charAt(i));
			}
		}
	}

	// work with parameters
	@Nullable
	public String getQueryParameter(@NotNull String key) {
		if (query == -1) {
			return null;
		}
		if (queryPositions == null) {
			parseQueryParameters();
		}
		return findParameter(raw, queryPositions, key);
	}

	@NotNull
	public List<String> getQueryParameters(@NotNull String key) {
		if (query == -1) {
			return emptyList();
		}
		if (queryPositions == null) {
			parseQueryParameters();
		}
		return findParameters(raw, queryPositions, key);
	}

	@NotNull
	public Iterable<QueryParameter> getQueryParametersIterable() {
		if (query == -1) {
			return emptyList();
		}
		if (queryPositions == null) {
			parseQueryParameters();
		}
		return () -> new QueryParamIterator(raw, queryPositions);
	}

	@NotNull
	public Map<String, String> getQueryParameters() {
		HashMap<String, String> map = new HashMap<>();
		for (QueryParameter queryParameter : getQueryParametersIterable()) {
			map.put(queryParameter.getKey(), queryParameter.getValue());
		}
		return map;
	}

	void parseQueryParameters() {
		int queryEnd = fragment == -1 ? raw.length() : fragment - 1;
		queryPositions = parseQueryParameters(raw, query, queryEnd);
	}

	private static final int[] NO_PARAMETERS = {};

	@NotNull
	static int[] parseQueryParameters(@NotNull String query, int pos, int end) {
		if (pos == end)
			return NO_PARAMETERS;
		assert query.length() >= end;
		assert pos != -1;
		assert query.length() <= 0xFFFF;

		int[] positions = new int[8];

		int k = 0;
		int keyStart = pos;
		while (keyStart < end) {
			int keyEnd = keyStart;
			while (keyEnd < end) {
				char c = query.charAt(keyEnd);
				if (c == '&' || c == '=') break;
				keyEnd++;
			}
			if (keyStart != keyEnd) {
				if (k >= positions.length) {
					positions = Arrays.copyOf(positions, positions.length * 2);
				}
				positions[k++] = keyStart | (keyEnd << 16);
			}
			while (keyStart < end) {
				if (query.charAt(keyStart++) == '&') break;
			}
		}

		return positions;
	}

	@NotNull
	public static Map<String, String> parseQueryIntoMap(@NotNull String query) {
		Map<String, String> result = new LinkedHashMap<>();

		int end = query.length();
		int keyStart = 0;
		while (keyStart < end) {
			int keyEnd = keyStart;
			while (keyEnd < end) {
				char c = query.charAt(keyEnd);
				if (c == '&' || c == '=') break;
				keyEnd++;
			}
			if (keyStart != keyEnd) {
				result.putIfAbsent(query.substring(keyStart, keyEnd), keyValueDecode(query, keyEnd));
			}
			while (keyStart < end) {
				if (query.charAt(keyStart++) == '&') break;
			}
		}

		return result;
	}

	@Nullable
	static String findParameter(@NotNull String src, @NotNull int[] parsedPositions, @NotNull String key) {
		for (int record : parsedPositions) {
			if (record == 0) break;
			int keyStart = record & 0xFFFF;
			int keyEnd = record >>> 16;
			if (isEqual(key, src, keyStart, keyEnd)) {
				return keyValueDecode(src, keyEnd);
			}
		}
		return null;
	}

	@NotNull
	static List<String> findParameters(@NotNull String src, @NotNull int[] parsedPositions, @NotNull String key) {
		List<String> container = new ArrayList<>();
		for (int record : parsedPositions) {
			if (record == 0) break;
			int keyStart = record & 0xFFFF;
			int keyEnd = record >>> 16;
			if (isEqual(key, src, keyStart, keyEnd)) {
				container.add(keyValueDecode(src, keyEnd));
			}
		}
		return container;
	}

	// work with path
	@NotNull
	String getPartialPath() {
		if (pos == -1 || pos > pathEnd) {
			return "/";
		}
		return raw.substring(pos, pathEnd);
	}

	String pollUrlPart() {
		if (pos < pathEnd) {
			int start = pos + 1;
			int nextSlash = raw.indexOf('/', start);
			pos = (short) (nextSlash > pathEnd ? pathEnd : nextSlash);
			String part;
			if (pos == -1) {
				part = raw.substring(start, pathEnd);
				pos = (short) raw.length();
			} else {
				part = raw.substring(start, pos);
			}
			return part;
		} else {
			return "";
		}
	}

	private static boolean isEqual(@NotNull String key, @NotNull String raw, int start, int end) {
		if (end - start != key.length()) {
			return false;
		}
		for (int i = 0; i < key.length(); i++) {
			if (key.charAt(i) != raw.charAt(start + i))
				return false;
		}
		return true;
	}

	private static int toInt(@NotNull String str, int pos, int end) throws ParseException {
		if (pos == end) {
			throw new ParseException(UrlParser.class, "Empty port value");
		}
		if ((end - pos) > 5) {
			throw new ParseException(UrlParser.class, "Bad port: " + str.substring(pos, end));
		}

		int result = 0;
		for (int i = pos; i < end; i++) {
			int c = str.charAt(i) - '0';
			if (c < 0 || c > 9)
				throw new ParseException(UrlParser.class, "Bad port: " + str.substring(pos, end));
			result = c + result * 10;
		}

		if (result > 0xFFFF) {
			throw new ParseException(UrlParser.class, "Bad port: " + str.substring(pos, end));
		}

		return result;
	}

	@Nullable
	private static String keyValueDecode(@NotNull String url, int keyEnd) {
		return urlDecode(url, keyEnd < url.length() && url.charAt(keyEnd) == '=' ? keyEnd + 1 : keyEnd);
	}

	/**
	 * Decodes a application/x-www-form-urlencoded string using a specific encoding scheme. The supplied
	 * encoding is used to determine what characters are represented by any consecutive sequences of the
	 * form "%xy".
	 *
	 * @param s string for decoding
	 * @return the newly decoded String
	 */
	@Nullable
	public static String urlDecode(@NotNull String s) {
		return urlDecode(s, 0);
	}

	@Nullable
	private static String urlDecode(String s, int pos) {
		int len = s.length();
		for (int i = pos; i < len; i++) {
			char c = s.charAt(i);
			if (c == '+' || c == '%')
				return urlDecode(s, pos, i); // inline hint
			if (c == '&' || c == '#')
				return s.substring(pos, i);
		}
		return s.substring(pos);
	}

	@Nullable
	private static String urlDecode(String s, int pos, int encodedSuffixPos) {
		int len = s.length();

		CachedBuffers cachedBuffers = CACHED_BUFFERS.get();
		if (cachedBuffers == null || cachedBuffers.bytes.length < len - pos) {
			int newCount = len - pos + (len - pos << 1);
			cachedBuffers = new CachedBuffers(new char[newCount], new byte[newCount]);
			CACHED_BUFFERS.set(cachedBuffers);
		}
		char[] chars = cachedBuffers.chars;
		byte[] bytes = cachedBuffers.bytes;

		int charsPos = 0;
		for (; pos < encodedSuffixPos; pos++) {
			chars[charsPos++] = s.charAt(pos);
		}
		try {
			LOOP:
			while (pos < len) {
				char c = s.charAt(pos);
				switch (c) {
					case '&':
					case '#':
						break LOOP;
					case '+':
						chars[charsPos++] = ' ';
						pos++;
						break;
					case '%':
						int bytesPos = 0;

						while ((pos + 2 < len) && (c == '%')) {
							bytes[bytesPos++] = (byte) ((parseHex(s.charAt(pos + 1)) << 4) + parseHex(s.charAt(pos + 2)));
							pos += 3;
							if (pos < len) {
								c = s.charAt(pos);
							}
						}

						if ((pos < len) && (c == '%'))
							return null;

						charsPos = ByteBufStrings.decodeUtf8(bytes, 0, bytesPos, chars, charsPos);
						break;
					default:
						chars[charsPos++] = c;
						pos++;
						break;
				}
			}
			return new String(chars, 0, charsPos);
		} catch (ParseException e) {
			return null;
		}
	}

	private static byte parseHex(char c) throws ParseException {
		if (c >= '0' && c <= '9') return (byte) (c - '0');
		if (c >= 'a' && c <= 'f') return (byte) (c - 'a' + 10);
		if (c >= 'A' && c <= 'F') return (byte) (c - 'A' + 10);
		throw new ParseException(UrlParser.class, "Failed to parse hex digit from '" + c + '\'');
	}

	@Override
	public String toString() {
		return raw;
	}
}
