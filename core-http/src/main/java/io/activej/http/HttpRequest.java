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
import io.activej.http.HttpHeaderValue.HttpHeaderValueOfSimpleCookies;
import io.activej.http.MultipartByteBufsDecoder.AsyncMultipartDataHandler;
import io.activej.promise.Promise;
import io.activej.promise.ToPromise;
import org.jetbrains.annotations.Contract;
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
import static io.activej.http.HttpHeaders.*;
import static io.activej.http.HttpMethod.*;

/**
 * Represents the HTTP request which {@link IHttpClient} sends to
 * {@link HttpServer}. It must have only one owner in each  part of time.
 * After creating an {@link HttpResponse} in a server, it will be recycled and
 * can not be used later.
 * <p>
 * {@code HttpRequest} class provides methods which can be used intuitively for
 * creating and configuring an HTTP request.
 */
public final class HttpRequest extends HttpMessage implements ToPromise<HttpRequest> {
	private static final boolean CHECKS = Checks.isEnabled(HttpRequest.class);

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

	HttpRequest(HttpVersion version, HttpMethod method, UrlParser url, @Nullable HttpServerConnection connection) {
		super(version);
		this.method = method;
		this.url = url;
		this.connection = connection;
	}

	public static Builder builder(HttpMethod method, String url) {
		UrlParser urlParser = UrlParser.of(url);
		return new HttpRequest(HttpVersion.HTTP_1_1, method, urlParser, null).new Builder();
	}

	public static Builder get(String url) {
		return builder(GET, url);
	}

	public static Builder post(String url) {
		return builder(POST, url);
	}

	public static Builder put(String url) {
		return builder(PUT, url);
	}

	@Override
	public Promise<HttpRequest> toPromise() {
		return Promise.of(this);
	}

	public final class Builder extends HttpMessage.Builder<Builder, HttpRequest> {
		private Builder() {}

		@Override
		protected void addCookies(List<HttpCookie> cookies) {
			headers.add(COOKIE, new HttpHeaderValueOfSimpleCookies(cookies));
		}

		@Override
		protected void addCookie(HttpCookie cookie) {
			addCookies(List.of(cookie));
		}
	}

	@Contract(pure = true)
	public HttpMethod getMethod() {
		return method;
	}

	@Contract(pure = true)
	public InetAddress getRemoteAddress() {
		// it makes sense to call this method only on server
		if (CHECKS) checkNotNull(remoteAddress);
		return remoteAddress;
	}

	void setRemoteAddress(InetAddress inetAddress) {
		if (CHECKS) checkState(!isRecycled());
		remoteAddress = inetAddress;
	}

	boolean isRemoteAddressSet() {
		return remoteAddress != null;
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
		if (CHECKS) checkState(!isRecycled());
		return url.getHostAndPort();
	}

	public String getPath() {
		if (CHECKS) checkState(!isRecycled());
		return url.getPath();
	}

	public String getPathAndQuery() {
		if (CHECKS) checkState(!isRecycled());
		return url.getPathAndQuery();
	}

	private @Nullable Map<String, String> parsedCookies;

	public Map<String, String> getCookies() {
		if (CHECKS) checkState(!isRecycled());
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

	public @Nullable String getCookie(String cookie) {
		if (CHECKS) checkState(!isRecycled());
		return getCookies().get(cookie);
	}

	public String getQuery() {
		if (CHECKS) checkState(!isRecycled());
		return url.getQuery();
	}

	public String getFragment() {
		if (CHECKS) checkState(!isRecycled());
		return url.getFragment();
	}

	public Map<String, String> getQueryParameters() {
		if (CHECKS) checkState(!isRecycled());
		if (queryParameters != null) {
			return queryParameters;
		}
		queryParameters = url.getQueryParameters();
		return queryParameters;
	}

	public @Nullable String getQueryParameter(String key) {
		if (CHECKS) checkState(!isRecycled());
		return url.getQueryParameter(key);
	}

	public List<String> getQueryParameters(String key) {
		if (CHECKS) checkState(!isRecycled());
		return url.getQueryParameters(key);
	}

	public Iterable<QueryParameter> getQueryParametersIterable() {
		if (CHECKS) checkState(!isRecycled());
		return url.getQueryParametersIterable();
	}

	public @Nullable String getPostParameter(String name) {
		if (CHECKS) checkState(!isRecycled());
		return getPostParameters().get(name);
	}

	public Map<String, String> getPostParameters() {
		if (CHECKS) checkState(!isRecycled());
		if (postParameters != null) return postParameters;
		if (body == null) throw new NullPointerException("Body must be loaded to decode post parameters");
		return postParameters =
			containsPostParameters() ?
				UrlParser.parseQueryIntoMap(body.array(), body.head(), body.tail()) :
				Map.of();
	}

	public boolean containsPostParameters() {
		if (CHECKS) checkState(!isRecycled());
		if (method != POST && method != PUT) {
			return false;
		}
		String contentType = getHeader(CONTENT_TYPE);
		return contentType != null && contentType.startsWith("application/x-www-form-urlencoded");
	}

	public boolean containsMultipartData() {
		if (CHECKS) checkState(!isRecycled());
		if (method != POST && method != PUT) {
			return false;
		}
		String contentType = getHeader(CONTENT_TYPE);
		return contentType != null && contentType.startsWith("multipart/form-data; boundary=");
	}

	public Map<String, String> getPathParameters() {
		if (CHECKS) checkState(!isRecycled());
		return pathParameters != null ? pathParameters : Map.of();
	}

	public String getPathParameter(String key) {
		if (CHECKS) checkState(!isRecycled());
		if (pathParameters != null) {
			String pathParameter = pathParameters.get(key);
			if (pathParameter != null) {
				return pathParameter;
			}
		}
		throw new IllegalArgumentException("No path parameter '" + key + "' found");
	}

	public Promise<Void> handleMultipart(AsyncMultipartDataHandler multipartDataHandler) {
		if (CHECKS) checkState(!isRecycled());
		String contentType = getHeader(CONTENT_TYPE);
		if (contentType == null || !contentType.startsWith("multipart/form-data; boundary=")) {
			return Promise.ofException(HttpError.ofCode(400, "Content type is not multipart/form-data"));
		}
		String boundary = contentType.substring(30);
		if (boundary.startsWith("\"") && boundary.endsWith("\"")) {
			boundary = boundary.substring(1, boundary.length() - 1);
		}
		return MultipartByteBufsDecoder.create(boundary)
			.split(takeBodyStream(), multipartDataHandler);
	}

	int getPos() {
		if (CHECKS) checkState(!isRecycled());
		return url.pos;
	}

	void setPos(int pos) {
		if (CHECKS) checkState(!isRecycled());
		url.pos = (short) pos;
	}

	public String getRelativePath() {
		if (CHECKS) checkState(!isRecycled());
		String partialPath = url.getPartialPath();
		return partialPath.startsWith("/") ? partialPath.substring(1) : partialPath; // strip first '/'
	}

	String pollUrlPart() {
		if (CHECKS) checkState(!isRecycled());
		return url.pollUrlPart();
	}

	void removePathParameter(String key) {
		if (CHECKS) checkState(!isRecycled());
		pathParameters.remove(key);
	}

	void putPathParameter(String key, String value) {
		if (CHECKS) checkState(!isRecycled());
		if (pathParameters == null) {
			pathParameters = new HashMap<>();
		}
		pathParameters.put(key, value);
	}

	@Override
	protected int estimateSize() {
		return
			estimateSize(
				LONGEST_HTTP_METHOD_SIZE
				+ 1 // SPACE
				+ url.getPathAndQueryLength()
			)
			+ HTTP_1_1_SIZE;
	}

	@Override
	protected void writeTo(ByteBuf buf) {
		method.write(buf);
		buf.put(SP);
		url.writePathAndQuery(buf);
		buf.put(HTTP_1_1);
		writeHeaders(buf);
	}

	public String getFullUrl() {
		if (CHECKS) checkState(!isRecycled());

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
