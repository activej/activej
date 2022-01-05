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
import io.activej.csp.ChannelSupplier;
import io.activej.http.HttpHeaderValue.HttpHeaderValueOfSimpleCookies;
import io.activej.http.MultipartDecoder.MultipartDataHandler;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.activej.bytebuf.ByteBufStrings.SP;
import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.Checks.checkState;
import static io.activej.common.Utils.nonNullElseEmpty;
import static io.activej.http.AbstractHttpConnection.WEB_SOCKET_VERSION;
import static io.activej.http.HttpClientConnection.CONNECTION_UPGRADE_HEADER;
import static io.activej.http.HttpClientConnection.UPGRADE_WEBSOCKET_HEADER;
import static io.activej.http.HttpHeaders.*;
import static io.activej.http.HttpMethod.*;
import static io.activej.http.Protocol.WS;
import static io.activej.http.Protocol.WSS;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

/**
 * Represents the HTTP request which {@link AsyncHttpClient} sends to
 * {@link AsyncHttpServer}. It must have only one owner in each  part of time.
 * After creating an {@link HttpResponse} in a server, it will be recycled and
 * can not be used later.
 * <p>
 * {@code HttpRequest} class provides methods which can be used intuitively for
 * creating and configuring an HTTP request.
 */
public final class HttpRequest extends HttpMessage implements WithInitializer<HttpRequest> {
	private static final boolean CHECK = Checks.isEnabled(HttpRequest.class);

	private static final int LONGEST_HTTP_METHOD_SIZE = 12;
	private static final byte[] HTTP_1_1 = encodeAscii(" HTTP/1.1");
	private static final int HTTP_1_1_SIZE = HTTP_1_1.length;
	private final HttpMethod method;
	private final UrlParser url;
	private final HttpServerConnection connection;
	private InetAddress remoteAddress;
	private Map<String, String> pathParameters;
	private Map<String, String> queryParameters;
	private Map<String, String> postParameters;

	// region creators
	HttpRequest(@NotNull HttpVersion version, @NotNull HttpMethod method, @NotNull UrlParser url, @Nullable HttpServerConnection connection) {
		super(version);
		this.method = method;
		this.url = url;
		this.connection = connection;
	}

	public static @NotNull HttpRequest of(@NotNull HttpMethod method, @NotNull String url) {
		UrlParser urlParser = UrlParser.of(url);
		HttpRequest request = new HttpRequest(HttpVersion.HTTP_1_1, method, urlParser, null);
		String hostAndPort = urlParser.getHostAndPort();
		if (hostAndPort != null) {
			request.headers.add(HOST, HttpHeaderValue.of(hostAndPort));
		}
		Protocol protocol = urlParser.getProtocol();
		if (protocol == WS || protocol == WSS) {
			request.addHeader(CONNECTION, CONNECTION_UPGRADE_HEADER);
			request.addHeader(HttpHeaders.UPGRADE, UPGRADE_WEBSOCKET_HEADER);
			request.addHeader(SEC_WEBSOCKET_VERSION, WEB_SOCKET_VERSION);
		}
		return request;
	}

	public static @NotNull HttpRequest get(@NotNull String url) {
		return HttpRequest.of(GET, url);
	}

	public static @NotNull HttpRequest post(@NotNull String url) {
		return HttpRequest.of(POST, url);
	}

	public static @NotNull HttpRequest put(@NotNull String url) {
		return HttpRequest.of(PUT, url);
	}

	public @NotNull HttpRequest withHeader(@NotNull HttpHeader header, @NotNull String value) {
		addHeader(header, value);
		return this;
	}

	public @NotNull HttpRequest withHeader(@NotNull HttpHeader header, byte[] value) {
		addHeader(header, value);
		return this;
	}

	public @NotNull HttpRequest withHeader(@NotNull HttpHeader header, @NotNull HttpHeaderValue value) {
		addHeader(header, value);
		return this;
	}

	public @NotNull HttpRequest withBody(byte[] array) {
		setBody(array);
		return this;
	}

	public @NotNull HttpRequest withBody(@NotNull ByteBuf body) {
		setBody(body);
		return this;
	}

	public @NotNull HttpRequest withBodyStream(@NotNull ChannelSupplier<ByteBuf> stream) {
		setBodyStream(stream);
		return this;
	}

	public @NotNull HttpRequest withCookies(@NotNull List<HttpCookie> cookies) {
		addCookies(cookies);
		return this;
	}

	public @NotNull HttpRequest withCookies(HttpCookie... cookie) {
		addCookies(cookie);
		return this;
	}

	public @NotNull HttpRequest withCookie(@NotNull HttpCookie cookie) {
		addCookie(cookie);
		return this;
	}

	public @NotNull HttpRequest withBodyGzipCompression() {
		setBodyGzipCompression();
		return this;
	}
	// endregion

	@Override
	public void addCookies(@NotNull List<HttpCookie> cookies) {
		if (CHECK) checkState(!isRecycled());
		headers.add(COOKIE, new HttpHeaderValueOfSimpleCookies(cookies));
	}

	@Override
	public void addCookie(@NotNull HttpCookie cookie) {
		if (CHECK) checkState(!isRecycled());
		addCookies(singletonList(cookie));
	}

	@Contract(pure = true)
	public @NotNull HttpMethod getMethod() {
		return method;
	}

	@Contract(pure = true)
	public InetAddress getRemoteAddress() {
		// it makes sense to call this method only on server
		if (CHECK) checkNotNull(remoteAddress);
		return remoteAddress;
	}

	void setRemoteAddress(@NotNull InetAddress inetAddress) {
		if (CHECK) checkState(!isRecycled());
		remoteAddress = inetAddress;
	}

	public Protocol getProtocol() {
		return url.getProtocol();
	}

	void setProtocol(Protocol protocol) {
		url.setProtocol(protocol);
	}

	@Override
	boolean isContentLengthExpected() {
		return method != GET && method != HEAD && method != TRACE && method != CONNECT && method != OPTIONS;
	}

	UrlParser getUrl() {
		return url;
	}

	public @Nullable String getHostAndPort() {
		if (CHECK) checkState(!isRecycled());
		return url.getHostAndPort();
	}

	public @NotNull String getPath() {
		if (CHECK) checkState(!isRecycled());
		return url.getPath();
	}

	public @NotNull String getPathAndQuery() {
		if (CHECK) checkState(!isRecycled());
		return url.getPathAndQuery();
	}

	private @Nullable Map<String, String> parsedCookies;

	public @NotNull Map<String, String> getCookies() {
		if (CHECK) checkState(!isRecycled());
		if (parsedCookies != null) {
			return parsedCookies;
		}
		Map<String, String> cookies = new LinkedHashMap<>();
		for (HttpCookie cookie : getHeader(COOKIE, HttpHeaderValue::toSimpleCookies)) {
			cookies.put(cookie.getName(), cookie.getValue());
		}
		return parsedCookies = cookies;
	}

	public HttpServerConnection getConnection() {
		return connection;
	}

	public @Nullable String getCookie(@NotNull String cookie) {
		if (CHECK) checkState(!isRecycled());
		return getCookies().get(cookie);
	}

	public @NotNull String getQuery() {
		if (CHECK) checkState(!isRecycled());
		return url.getQuery();
	}

	public @NotNull String getFragment() {
		if (CHECK) checkState(!isRecycled());
		return url.getFragment();
	}

	public @NotNull Map<String, String> getQueryParameters() {
		if (CHECK) checkState(!isRecycled());
		if (queryParameters != null) {
			return queryParameters;
		}
		queryParameters = url.getQueryParameters();
		return queryParameters;
	}

	public @Nullable String getQueryParameter(@NotNull String key) {
		if (CHECK) checkState(!isRecycled());
		return url.getQueryParameter(key);
	}

	public @NotNull List<String> getQueryParameters(@NotNull String key) {
		if (CHECK) checkState(!isRecycled());
		return url.getQueryParameters(key);
	}

	public @NotNull Iterable<QueryParameter> getQueryParametersIterable() {
		if (CHECK) checkState(!isRecycled());
		return url.getQueryParametersIterable();
	}

	public @Nullable String getPostParameter(String name) {
		if (CHECK) checkState(!isRecycled());
		return getPostParameters().get(name);
	}

	public @NotNull Map<String, String> getPostParameters() {
		if (CHECK) checkState(!isRecycled());
		if (postParameters != null) return postParameters;
		if (body == null) throw new NullPointerException("Body must be loaded to decode post parameters");
		return postParameters =
				containsPostParameters() ?
						UrlParser.parseQueryIntoMap(body.array(), body.head(), body.tail()) :
						emptyMap();
	}

	public boolean containsPostParameters() {
		if (CHECK) checkState(!isRecycled());
		if (method != POST && method != PUT) {
			return false;
		}
		String contentType = getHeader(CONTENT_TYPE);
		return contentType != null && contentType.startsWith("application/x-www-form-urlencoded");
	}

	public boolean containsMultipartData() {
		if (CHECK) checkState(!isRecycled());
		if (method != POST && method != PUT) {
			return false;
		}
		String contentType = getHeader(CONTENT_TYPE);
		return contentType != null && contentType.startsWith("multipart/form-data; boundary=");
	}

	public @NotNull Map<String, String> getPathParameters() {
		if (CHECK) checkState(!isRecycled());
		return pathParameters != null ? pathParameters : emptyMap();
	}

	public @NotNull String getPathParameter(@NotNull String key) {
		if (CHECK) checkState(!isRecycled());
		if (pathParameters != null) {
			String pathParameter = pathParameters.get(key);
			if (pathParameter != null) {
				return pathParameter;
			}
		}
		throw new IllegalArgumentException("No path parameter '" + key + "' found");
	}

	public Promise<Void> handleMultipart(MultipartDataHandler multipartDataHandler) {
		if (CHECK) checkState(!isRecycled());
		String contentType = getHeader(CONTENT_TYPE);
		if (contentType == null || !contentType.startsWith("multipart/form-data; boundary=")) {
			return Promise.ofException(HttpError.ofCode(400, "Content type is not multipart/form-data"));
		}
		String boundary = contentType.substring(30);
		if (boundary.startsWith("\"") && boundary.endsWith("\"")) {
			boundary = boundary.substring(1, boundary.length() - 1);
		}
		return MultipartDecoder.create(boundary)
				.split(getBodyStream(), multipartDataHandler);
	}

	int getPos() {
		if (CHECK) checkState(!isRecycled());
		return url.pos;
	}

	void setPos(int pos) {
		if (CHECK) checkState(!isRecycled());
		url.pos = (short) pos;
	}

	public @NotNull String getRelativePath() {
		if (CHECK) checkState(!isRecycled());
		String partialPath = url.getPartialPath();
		return partialPath.startsWith("/") ? partialPath.substring(1) : partialPath; // strip first '/'
	}

	String pollUrlPart() {
		if (CHECK) checkState(!isRecycled());
		return url.pollUrlPart();
	}

	void removePathParameter(String key) {
		if (CHECK) checkState(!isRecycled());
		pathParameters.remove(key);
	}

	void putPathParameter(String key, @NotNull String value) {
		if (CHECK) checkState(!isRecycled());
		if (pathParameters == null) {
			pathParameters = new HashMap<>();
		}
		pathParameters.put(key, value);
	}

	@Override
	protected int estimateSize() {
		return estimateSize(LONGEST_HTTP_METHOD_SIZE
				+ 1 // SPACE
				+ url.getPathAndQueryLength())
				+ HTTP_1_1_SIZE;
	}

	@Override
	protected void writeTo(@NotNull ByteBuf buf) {
		method.write(buf);
		buf.put(SP);
		url.writePathAndQuery(buf);
		buf.put(HTTP_1_1);
		writeHeaders(buf);
	}

	public String getFullUrl() {
		if (CHECK) checkState(!isRecycled());

		if (url.isRelativePath()) {
			String host = getHeader(HOST);
			return getProtocol().lowercase() + "://" + nonNullElseEmpty(host) + url;
		}
		return url.toString();
	}

	@Override
	public String toString() {
		if (isRecycled()) return "{Recycled HttpRequest}";

		return getFullUrl();
	}
}
