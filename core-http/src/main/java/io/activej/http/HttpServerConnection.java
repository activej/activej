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
import io.activej.common.concurrent.ThreadLocalCharArray;
import io.activej.common.exception.UncheckedException;
import io.activej.common.exception.parse.ParseException;
import io.activej.common.exception.parse.UnknownFormatException;
import io.activej.csp.ChannelSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncHttpServer.Inspector;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.net.socket.tcp.AsyncTcpSocketSsl;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.util.Arrays;

import static io.activej.async.process.AsyncCloseable.CLOSE_EXCEPTION;
import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.common.Checks.checkState;
import static io.activej.csp.ChannelSupplier.ofLazyProvider;
import static io.activej.csp.ChannelSuppliers.concat;
import static io.activej.eventloop.util.RunnableWithContext.wrapContext;
import static io.activej.http.HttpHeaders.CONNECTION;
import static io.activej.http.HttpMessage.MUST_LOAD_BODY;
import static io.activej.http.HttpMethod.*;
import static io.activej.http.HttpVersion.HTTP_1_0;
import static io.activej.http.HttpVersion.HTTP_1_1;
import static io.activej.http.Protocol.*;
import static io.activej.http.WebSocketConstants.UPGRADE_WITH_BODY;

/**
 * It represents server connection. It can receive {@link HttpRequest requests}
 * from {@link AsyncHttpClient clients} and respond to them with
 * {@link AsyncServlet<HttpRequest> async servlet}.
 */
public final class HttpServerConnection extends AbstractHttpConnection {
	private static final boolean CHECK = Checks.isEnabled(HttpServerConnection.class);

	private static final int HEADERS_SLOTS = 256;
	private static final int MAX_PROBINGS = 2;
	private static final HttpMethod[] METHODS = new HttpMethod[HEADERS_SLOTS];

	static {
		assert Integer.bitCount(METHODS.length) == 1;
		nxt:
		for (HttpMethod httpMethod : HttpMethod.values()) {
			int hashCode = Arrays.hashCode(httpMethod.bytes);
			for (int p = 0; p < MAX_PROBINGS; p++) {
				int slot = (hashCode + p) & (METHODS.length - 1);
				if (METHODS[slot] == null) {
					METHODS[slot] = httpMethod;
					continue nxt;
				}
			}
			throw new IllegalArgumentException("HTTP METHODS hash collision, try to increase METHODS size");
		}
	}

	private final InetAddress remoteAddress;

	@Nullable
	private HttpRequest request;
	private final AsyncHttpServer server;
	@Nullable
	private final Inspector inspector;
	private final AsyncServlet servlet;
	private final char[] charBuffer;

	private static final byte[] EXPECT_100_CONTINUE = encodeAscii("100-continue");
	private static final byte[] EXPECT_RESPONSE_CONTINUE = encodeAscii("HTTP/1.1 100 Continue\r\n\r\n");

	/**
	 * Creates a new instance of HttpServerConnection
	 *
	 * @param eventloop     eventloop which will handle its tasks
	 * @param remoteAddress an address of remote
	 * @param server        server, which uses this connection
	 * @param servlet       servlet for handling requests
	 */
	HttpServerConnection(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, InetAddress remoteAddress,
			AsyncHttpServer server, AsyncServlet servlet, char[] charBuffer) {
		super(eventloop, asyncTcpSocket, server.maxBodySize);
		this.remoteAddress = remoteAddress;
		this.server = server;
		this.servlet = servlet;
		this.inspector = server.inspector;
		this.charBuffer = charBuffer;
	}

	void serve() {
		(pool = server.poolNew).addLastNode(this);
		poolTimestamp = eventloop.currentTimeMillis();
		socket.read().whenComplete(startLineConsumer);
	}

	public PoolLabel getCurrentPool() {
		if (pool == server.poolNew) return PoolLabel.NEW;
		if (pool == server.poolKeepAlive) return PoolLabel.KEEP_ALIVE;
		if (pool == server.poolReadWrite) return PoolLabel.READ_WRITE;
		if (pool == server.poolServing) return PoolLabel.SERVING;
		return PoolLabel.NONE;
	}

	public InetAddress getRemoteAddress() {
		return remoteAddress;
	}

	@Override
	protected void onClosedWithError(@NotNull Throwable e) {
		if (inspector != null) {
			inspector.onHttpError(this, e);
		}
	}

	/**
	 * This method is called after received line of header.
	 *
	 * @param line received line of header.
	 */
	@SuppressWarnings("PointlessArithmeticExpression")
	@Override
	protected void onStartLine(byte[] line, int limit) throws ParseException {
		switchPool(server.poolReadWrite);

		HttpMethod method = getHttpMethod(line);
		if (method == null) {
			throw new UnknownFormatException(HttpServerConnection.class,
					"Unknown HTTP method. First Bytes: " + Arrays.toString(line));
		}

		int urlStart = method.size + 1;

		int urlEnd;
		for (urlEnd = urlStart; urlEnd < limit; urlEnd++) {
			if (line[urlEnd] == SP) {
				break;
			}
		}

		int p;
		for (p = urlEnd + 1; p < limit; p++) {
			if (line[p] != SP) {
				break;
			}
		}

		HttpVersion version;
		if (p + 7 < limit
				&& line[p + 0] == 'H' && line[p + 1] == 'T' && line[p + 2] == 'T' && line[p + 3] == 'P' && line[p + 4] == '/' && line[p + 5] == '1' && line[p + 6] == '.') {
			if (line[p + 7] == '1') {
				flags |= KEEP_ALIVE; // keep-alive for HTTP/1.1
				version = HTTP_1_1;
			} else if (line[p + 7] == '0') {
				version = HTTP_1_0;
			} else {
				throw new UnknownFormatException(HttpServerConnection.class,
						"Unknown HTTP version. First Bytes: " + Arrays.toString(line));
			}
		} else {
			throw new UnknownFormatException(HttpServerConnection.class,
					"Unsupported HTTP version. First Bytes: " + Arrays.toString(line));
		}

		request = new HttpRequest(version, method,
				UrlParser.parse(decodeAscii(line, urlStart, urlEnd - urlStart, ThreadLocalCharArray.ensure(charBuffer, urlEnd - urlStart))), this);
		request.maxBodySize = maxBodySize;

		if (method == GET || method == DELETE) {
			contentLength = 0;
		}
	}

	@Override
	protected void onHeaderBuf(ByteBuf buf) {
		//noinspection ConstantConditions
		request.addHeaderBuf(buf);
	}

	private static HttpMethod getHttpMethod(byte[] line) {
		boolean get = line[0] == 'G' && line[1] == 'E' && line[2] == 'T' && (line[3] == SP || line[3] == HT);
		if (get) {
			return GET;
		}
		boolean post = line[0] == 'P' && line[1] == 'O' && line[2] == 'S' && line[3] == 'T' && (line[4] == SP || line[4] == HT);
		if (post) {
			return POST;
		}
		return getHttpMethodFromMap(line);
	}

	private static HttpMethod getHttpMethodFromMap(byte[] line) {
		int hashCode = 1;
		for (int i = 0; i < 10; i++) {
			byte b = line[i];
			if (b == SP || b == HT) {
				for (int p = 0; p < MAX_PROBINGS; p++) {
					int slot = (hashCode + p) & (METHODS.length - 1);
					HttpMethod method = METHODS[slot];
					if (method == null) {
						break;
					}
					if (method.compareTo(line, 0, i)) {
						return method;
					}
				}
				return null;
			}
			hashCode = 31 * hashCode + b;
		}
		return null;
	}

	/**
	 * This method is called after receiving header. It sets its value to request.
	 *
	 * @param header received header
	 */
	@Override
	protected void onHeader(HttpHeader header, byte[] array, int off, int len) throws ParseException {
		if (header == HttpHeaders.EXPECT && equalsLowerCaseAscii(EXPECT_100_CONTINUE, array, off, len)) {
			socket.write(ByteBuf.wrapForReading(EXPECT_RESPONSE_CONTINUE));
		}
		//noinspection ConstantConditions
		if (request.headers.size() >= MAX_HEADERS) {
			throw TOO_MANY_HEADERS;
		}
		request.addHeader(header, array, off, len);
	}

	private void writeHttpResponse(HttpResponse httpResponse) {
		boolean isWebSocket = isWebSocket();
		if (!isWebSocket || httpResponse.getCode() != 101) {
			HttpHeaderValue connectionHeader = (flags & KEEP_ALIVE) != 0 ? CONNECTION_KEEP_ALIVE_HEADER : CONNECTION_CLOSE_HEADER;
			if (server.keepAliveTimeoutMillis == 0 ||
					++numberOfKeepAliveRequests >= server.maxKeepAliveRequests && server.maxKeepAliveRequests != 0) {
				connectionHeader = CONNECTION_CLOSE_HEADER;
			}
			httpResponse.addHeader(CONNECTION, connectionHeader);

			if (isWebSocket) {
				// if web socket upgrade request was unsuccessful, it is not a web socket connection
				flags &= ~WEB_SOCKET;
			}
		}
		ByteBuf buf = renderHttpMessage(httpResponse);
		if (buf != null) {
			if ((flags & KEEP_ALIVE) != 0) {
				eventloop.post(wrapContext(this, () -> writeBuf(buf)));
			} else {
				writeBuf(buf);
			}
		} else {
			writeHttpMessageAsStream(httpResponse);
		}
		httpResponse.recycle();
	}

	@Override
	protected void onHeadersReceived(@Nullable ByteBuf body, @Nullable ChannelSupplier<ByteBuf> bodySupplier) {
		assert !isClosed();

		//noinspection ConstantConditions
		request.flags |= MUST_LOAD_BODY;
		request.body = body;
		request.bodyStream = bodySupplier;
		if (isWebSocket()) {
			if (!processWebSocketRequest(body)) return;
		} else {
			request.setProtocol(socket instanceof AsyncTcpSocketSsl ? HTTPS : HTTP);
		}
		request.setRemoteAddress(remoteAddress);

		if (inspector != null) inspector.onHttpRequest(request);

		switchPool(server.poolServing);

		HttpRequest request = this.request;
		Promise<HttpResponse> servletResult;
		try {
			servletResult = servlet.serveAsync(request);
		} catch (UncheckedException u) {
			servletResult = Promise.ofException(u.getCause());
		}
		servletResult.whenComplete((response, e) -> {
			if (CHECK) checkState(eventloop.inEventloopThread());
			if (isClosed()) {
				request.recycle();
				if (response != null) {
					response.recycle();
				}
				return;
			}
			switchPool(server.poolReadWrite);
			if (e == null) {
				if (inspector != null) {
					inspector.onHttpResponse(request, response);
				}
				writeHttpResponse(response);
			} else {
				if (inspector != null) {
					inspector.onServletException(request, e);
				}
				writeException(e);
			}

			request.recycle();
		});
	}

	@SuppressWarnings("ConstantConditions")
	private boolean processWebSocketRequest(@Nullable ByteBuf body) {
		if (body != null && body.readRemaining() == 0) {
			ChannelSupplier<ByteBuf> ofQueueSupplier = ofLazyProvider(() -> isClosed() ?
					ChannelSupplier.ofException(CLOSE_EXCEPTION) :
					ChannelSupplier.of(readQueue.takeRemaining()));
			ChannelSupplier<ByteBuf> ofSocketSupplier = ChannelSupplier.ofSocket(socket);
			request.bodyStream = concat(ofQueueSupplier, ofSocketSupplier)
					.withEndOfStream(eos -> eos.whenException(this::closeWebSocketConnection));
			request.setProtocol(socket instanceof AsyncTcpSocketSsl ? WSS : WS);
			request.maxBodySize = server.maxWebSocketMessageSize;
			return true;
		} else {
			closeWithError(UPGRADE_WITH_BODY);
			return false;
		}
	}

	@Override
	protected void onBodyReceived() {
		assert !isClosed();
		flags |= BODY_RECEIVED;
		if ((flags & BODY_SENT) != 0 && pool != server.poolServing) {
			onHttpMessageComplete();
		}
	}

	@Override
	protected void onBodySent() {
		assert !isClosed();
		flags |= BODY_SENT;
		if ((flags & BODY_RECEIVED) != 0 && pool != server.poolServing) {
			onHttpMessageComplete();
		}
	}

	@Override
	protected void onNoContentLength() {
		throw new AssertionError("This method should not be called on a server");
	}

	private void onHttpMessageComplete() {
		assert !isClosed();
		if (isWebSocket()) return;

		if ((flags & KEEP_ALIVE) != 0 && server.keepAliveTimeoutMillis != 0) {
			switchPool(server.poolKeepAlive);
			flags = 0;
			try {
				/*
					as per RFC 7230, section 3.3.3,
					if no Content-Length header is set, server can assume that a length of a message is 0
				 */
				contentLength = 0;
				readHttpMessage();
			} catch (ParseException e) {
				closeWithError(e);
			}
		} else {
			close();
		}
	}

	private void writeException(Throwable e) {
		writeHttpResponse(server.formatHttpError(e));
	}

	@Override
	protected void onClosed() {
		if (request != null && pool != server.poolServing) {
			request.recycle();
			request = null;
		}
		//noinspection ConstantConditions
		pool.removeNode(this);
		//noinspection AssertWithSideEffects,ConstantConditions
		assert (pool = null) == null;
		server.onConnectionClosed();
	}

	@Override
	public String toString() {
		return "HttpServerConnection{" +
				"pool=" + getCurrentPool() +
				", remoteAddress=" + remoteAddress +
				',' + super.toString() +
				'}';
	}
}
