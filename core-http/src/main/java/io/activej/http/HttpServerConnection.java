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
import io.activej.common.ApplicationSettings;
import io.activej.common.Checks;
import io.activej.common.MemSize;
import io.activej.common.Utils;
import io.activej.common.recycle.Recyclable;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.http.HttpServer.Inspector;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.net.socket.tcp.SslTcpSocket;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.common.Utils.nullify;
import static io.activej.common.exception.FatalErrorHandler.handleError;
import static io.activej.csp.supplier.ChannelSuppliers.concat;
import static io.activej.http.HttpHeaderValue.ofBytes;
import static io.activej.http.HttpHeaderValue.ofDecimal;
import static io.activej.http.HttpHeaders.*;
import static io.activej.http.HttpMessage.MUST_LOAD_BODY;
import static io.activej.http.HttpMethod.DELETE;
import static io.activej.http.HttpMethod.GET;
import static io.activej.http.HttpVersion.HTTP_1_0;
import static io.activej.http.HttpVersion.HTTP_1_1;
import static io.activej.http.Protocol.*;
import static io.activej.http.WebSocketConstants.UPGRADE_WITH_BODY;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * It represents server connection. It can receive {@link HttpRequest requests}
 * from {@link IHttpClient clients} and respond to them with
 * {@link AsyncServlet<HttpRequest> async servlet}.
 */
public final class HttpServerConnection extends AbstractHttpConnection {
	private static final boolean CHECKS = Checks.isEnabled(HttpServerConnection.class);

	private static final boolean DETAILED_ERROR_MESSAGES = ApplicationSettings.getBoolean(HttpServerConnection.class, "detailedErrorMessages", false);
	private static final int INITIAL_WRITE_BUFFER_SIZE = ApplicationSettings.getMemSize(HttpServerConnection.class, "initialWriteBufferSize", MemSize.ZERO).toInt();

	private static final HttpMethod[] METHODS = new HttpMethod[128];

	static {
		assert Integer.bitCount(METHODS.length) == 1;
		for (HttpMethod httpMethod : HttpMethod.values()) {
			int hashCode = 0;
			for (int i = 0; i < httpMethod.bytes.length; i++) {
				hashCode += httpMethod.bytes[i];
			}
			int slot = hashCode & (METHODS.length - 1);
			if (METHODS[slot] != null)
				throw new IllegalArgumentException("HTTP METHODS hash collision, try to increase METHODS size");
			METHODS[slot] = httpMethod;
		}
	}

	private final InetAddress remoteAddress;

	private final HttpServer server;
	private final AsyncServlet servlet;
	private @Nullable HttpRequest request;
	private final @Nullable Inspector inspector;

	private @Nullable ByteBuf writeBuf;

	private static final byte[] EXPECT_100_CONTINUE = encodeAscii("100-continue");
	private static final byte[] EXPECT_RESPONSE_CONTINUE = encodeAscii("""
		HTTP/1.1 100 Continue\r
		\r
		""");
	private static final byte[] MALFORMED_HTTP_RESPONSE = encodeAscii("""
		HTTP/1.1 400 Bad Request\r
		Connection: close\r
		Content-Length: 0\r
		\r
		""");

	/**
	 * Creates a new instance of HttpServerConnection
	 *
	 * @param reactor       reactor which will handle its tasks
	 * @param remoteAddress an address of remote
	 * @param server        server, which uses this connection
	 * @param servlet       servlet for handling requests
	 */
	HttpServerConnection(
		Reactor reactor, ITcpSocket socket, InetAddress remoteAddress, HttpServer server, AsyncServlet servlet
	) {
		super(reactor, socket, server.maxBodySize);
		this.remoteAddress = remoteAddress;
		this.server = server;
		this.servlet = servlet;
		this.inspector = server.inspector;
	}

	void serve() {
		if (inspector != null) inspector.onAccept(this);
		(pool = server.poolNew).addLastNode(this);
		poolTimestamp = reactor.currentTimeMillis();
		socket.read().subscribe(readMessageConsumer);
	}

	@Override
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

	@Nullable HttpRequest getRequest() {
		return request;
	}

	@Override
	protected void readMessage() throws MalformedHttpException {
		do {
			request = nullify(request, HttpMessage::recycle); // nullify any previous request
			contentLength = 0L; // RFC 7230, section 3.3.3: if no Content-Length header is set, server can assume that a length of a message is 0
			flags = READING_MESSAGES;
			readStartLine();
			if (isClosed()) return;
		} while ((flags & (KEEP_ALIVE | BODY_RECEIVED | BODY_SENT)) == (KEEP_ALIVE | BODY_RECEIVED | BODY_SENT) && readBuf != null);
		flags &= ~READING_MESSAGES;
		if (writeBuf != null) {
			ByteBuf writeBuf = this.writeBuf;
			this.writeBuf = null;
			writeBuf(writeBuf);
		} else if ((flags & (BODY_RECEIVED | BODY_SENT)) == (BODY_RECEIVED | BODY_SENT)) {
			onHttpMessageComplete();
		}
	}

	@Override
	protected void onClosedWithError(Exception e) {
		if (inspector != null) {
			inspector.onHttpError(this, e);
		}
	}

	@Override
	protected void onMalformedHttpException(MalformedHttpException e) {
		if (inspector != null) {
			inspector.onMalformedHttpRequest(this, e, readBuf == null ? new byte[0] : readBuf.getArray());
		}

		writeBuf = ensureWriteBuffer(MALFORMED_HTTP_RESPONSE.length);
		this.writeBuf.put(MALFORMED_HTTP_RESPONSE);
		ByteBuf writeBuf = this.writeBuf;
		this.writeBuf = null;

		socket.write(writeBuf)
			.whenComplete(this::close);
	}

	/**
	 * This method is called after received line of header.
	 *
	 * @param line received line of header.
	 */
	@SuppressWarnings("PointlessArithmeticExpression")
	@Override
	protected void onStartLine(byte[] line, int pos, int limit) throws MalformedHttpException {
		switchPool(server.poolReadWrite);

		HttpMethod method = getHttpMethod(line, pos);
		if (method == null) {
			if (!DETAILED_ERROR_MESSAGES) throw new MalformedHttpException("Unknown HTTP method");
			throw new MalformedHttpException(
				"Unknown HTTP method. First line: " +
				new String(line, 0, limit, ISO_8859_1));
		}

		int urlStart = pos + method.size + 1;

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
		if (p + 7 < limit &&
			line[p + 0] == 'H' &&
			line[p + 1] == 'T' &&
			line[p + 2] == 'T' &&
			line[p + 3] == 'P' &&
			line[p + 4] == '/' &&
			line[p + 5] == '1' &&
			line[p + 6] == '.'
		) {
			if (line[p + 7] == '1') {
				flags |= KEEP_ALIVE; // keep-alive for HTTP/1.1
				version = HTTP_1_1;
			} else if (line[p + 7] == '0') {
				version = HTTP_1_0;
			} else {
				if (!DETAILED_ERROR_MESSAGES) throw new MalformedHttpException("Unknown HTTP version");
				throw new MalformedHttpException(
					"Unknown HTTP version. First line: " +
					new String(line, 0, limit, ISO_8859_1));
			}
		} else {
			if (!DETAILED_ERROR_MESSAGES) throw new MalformedHttpException("Unsupported HTTP version");
			throw new MalformedHttpException(
				"Unsupported HTTP version. First line: " + new String(line, 0, limit, ISO_8859_1));
		}

		request = new HttpRequest(version, method, UrlParser.parse(line, urlStart, urlEnd), this);
		request.maxBodySize = maxBodySize;

		if (method == GET || method == DELETE) {
			contentLength = 0L;
		}
	}

	private static HttpMethod getHttpMethod(byte[] line, int pos) {
		boolean get = line[pos] == 'G' && line[pos + 1] == 'E' && line[pos + 2] == 'T' && (line[pos + 3] == SP || line[pos + 3] == HT);
		if (get) {
			return GET;
		}
		return getHttpMethodFromMap(line, pos);
	}

	private static HttpMethod getHttpMethodFromMap(byte[] line, int pos) {
		int hashCode = 0;
		for (int i = pos; i < min(pos + 10, line.length); i++) {
			byte b = line[i];
			if (b == SP || b == HT) {
				int slot = hashCode & (METHODS.length - 1);
				HttpMethod method = METHODS[slot];
				return method != null && method.compareTo(line, pos, i - pos) ? method : null;
			}
			hashCode += b;
		}
		return null;
	}

	/**
	 * This method is called after receiving header. It sets its value to request.
	 *
	 * @param header received header
	 */
	@Override
	protected void onHeader(HttpHeader header, byte[] array, int off, int len) throws MalformedHttpException {
		if (header == HttpHeaders.EXPECT && equalsLowerCaseAscii(EXPECT_100_CONTINUE, array, off, len)) {
			socket.write(ByteBuf.wrapForReading(EXPECT_RESPONSE_CONTINUE));
		}
		//noinspection ConstantConditions
		if (request.headers.size() >= MAX_HEADERS) {
			throw new MalformedHttpException("Too many headers");
		}
		request.headers.add(header, ofBytes(array, off, len));
	}

	private void writeHttpResponse(HttpResponse httpResponse) {
		boolean isWebSocket = IWebSocket.ENABLED && isWebSocket();
		if (!isWebSocket || httpResponse.getCode() != 101) {
			httpResponse.headers.addIfAbsent(CONNECTION, () -> {
					if ((flags & KEEP_ALIVE) == 0 ||
						server.keepAliveTimeoutMillis == 0 ||
						numberOfRequests >= server.maxKeepAliveRequests && server.maxKeepAliveRequests != 0
					) {
						return CONNECTION_CLOSE_HEADER;
					}
					return CONNECTION_KEEP_ALIVE_HEADER;
				});

			if (isWebSocket) {
				// if web socket upgrade request was unsuccessful, it is not a web socket connection
				flags &= ~WEB_SOCKET;
			}
		}
		if (renderHttpResponse(httpResponse)) {
			if ((flags & READING_MESSAGES) != 0) {
				flags |= BODY_SENT;
			} else {
				ByteBuf writeBuf = this.writeBuf;
				this.writeBuf = null;
				writeBuf(writeBuf);
			}
		} else {
			ByteBuf writeBuf = this.writeBuf;
			this.writeBuf = null;
			writeHttpMessageAsStream(writeBuf, httpResponse);
		}
	}

	boolean renderHttpResponse(HttpMessage httpMessage) {
		if (httpMessage.body != null) {
			ByteBuf body = httpMessage.body;
			httpMessage.body = null;
			if ((httpMessage.flags & HttpMessage.USE_GZIP) == 0) {
				httpMessage.headers.addIfAbsent(CONTENT_LENGTH, () -> ofDecimal(body.readRemaining()));
				int messageSize = httpMessage.estimateSize() + body.readRemaining();
				writeBuf = ensureWriteBuffer(messageSize);
				httpMessage.writeTo(writeBuf);
				writeBuf.put(body);
				body.recycle();
			} else {
				ByteBuf gzippedBody = GzipProcessorUtils.toGzip(body);
				httpMessage.headers.addIfAbsent(CONTENT_ENCODING, CONTENT_ENCODING_GZIP_HEADER);
				httpMessage.headers.addIfAbsent(CONTENT_LENGTH, () -> ofDecimal(gzippedBody.readRemaining()));
				int messageSize = httpMessage.estimateSize() + gzippedBody.readRemaining();
				writeBuf = ensureWriteBuffer(messageSize);
				httpMessage.writeTo(writeBuf);
				writeBuf.put(gzippedBody);
				gzippedBody.recycle();
			}
			return true;
		}

		if (httpMessage.bodyStream == null) {
			if (httpMessage.isContentLengthExpected()) {
				httpMessage.headers.addIfAbsent(CONTENT_LENGTH, ZERO_HEADER);
			}
			writeBuf = ensureWriteBuffer(httpMessage.estimateSize());
			httpMessage.writeTo(writeBuf);
			return true;
		}

		return false;
	}

	private ByteBuf ensureWriteBuffer(int messageSize) {
		return writeBuf == null ?
			ByteBufPool.allocate(Math.max(messageSize, INITIAL_WRITE_BUFFER_SIZE)) :
			ByteBufPool.ensureWriteRemaining(writeBuf, messageSize);
	}

	@Override
	protected void onHeadersReceived(@Nullable ByteBuf body, @Nullable ChannelSupplier<ByteBuf> bodySupplier) {
		assert !isClosed();

		//noinspection ConstantConditions
		request.flags |= MUST_LOAD_BODY;
		request.body = body;
		request.bodyStream = bodySupplier == null ? null : sanitize(bodySupplier);
		if (IWebSocket.ENABLED && isWebSocket()) {
			if (!processWebSocketRequest(body)) return;
		} else {
			request.setProtocol(socket instanceof SslTcpSocket ? HTTPS : HTTP);
		}
		request.setRemoteAddress(remoteAddress);

		numberOfRequests++;
		if (inspector != null) inspector.onHttpRequest(request);

		switchPool(server.poolServing);

		HttpRequest request = this.request;
		Promise<HttpResponse> servletResult;
		try {
			servletResult = servlet.serve(request);
		} catch (Exception e) {
			handleError(e, this);
			servletResult = Promise.ofException(e);
		}
		servletResult.subscribe((response, e) -> {
			if (CHECKS) checkInReactorThread(this);
			if (isClosed()) {
				request.recycle();
				readBuf = nullify(readBuf, ByteBuf::recycle);
				stashedBufs = nullify(stashedBufs, Recyclable::recycle);
				if (response != null) {
					response.recycleBody();
				}
				return;
			}

			switchPool(server.poolReadWrite);
			if (e == null) {
				if (inspector != null) {
					inspector.onHttpResponse(request, response);
				}
				recycle();
				writeHttpResponse(response);
			} else {
				if (inspector != null) {
					inspector.onServletException(request, e);
				}
				recycle();
				writeException(e);
			}
		});
	}

	private void recycle() {
		if (stashedBufs != null) {
			stashedBufs.recycle();
			stashedBufs = null;
		}
		if (readBuf != null && !readBuf.canRead()) {
			readBuf.recycle();
			readBuf = null;
		}
		//noinspection ConstantConditions
		request.recycle();
	}

	@SuppressWarnings("ConstantConditions")
	private boolean processWebSocketRequest(@Nullable ByteBuf body) {
		if (body != null && body.readRemaining() == 0) {
			ChannelSupplier<ByteBuf> ofReadBufSupplier = ChannelSuppliers.ofValue(detachReadBuf());
			ChannelSupplier<ByteBuf> ofSocketSupplier = ChannelSuppliers.ofSocket(socket);
			request.bodyStream = sanitize(concat(ofReadBufSupplier, ofSocketSupplier)
				.withEndOfStream(eos -> eos.whenException(this::closeWebSocketConnection)));
			request.setProtocol(socket instanceof SslTcpSocket ? WSS : WS);
			request.maxBodySize = server.maxWebSocketMessageSize;
			return true;
		} else {
			closeEx(UPGRADE_WITH_BODY);
			return false;
		}
	}

	@Override
	protected void onBodyReceived() {
		assert !isClosed();
		flags |= BODY_RECEIVED;
		if ((flags & (READING_MESSAGES | BODY_RECEIVED | BODY_SENT)) == (BODY_RECEIVED | BODY_SENT) &&
			pool != server.poolServing
		) {
			onHttpMessageComplete();
		}
	}

	@Override
	protected void onBodySent() {
		assert !isClosed();
		flags |= BODY_SENT;
		if ((flags & (READING_MESSAGES | BODY_RECEIVED | BODY_SENT)) == (BODY_RECEIVED | BODY_SENT) &&
			pool != server.poolServing
		) {
			onHttpMessageComplete();
		}
	}

	@Override
	protected void onNoContentLength() {
		throw new AssertionError("This method should not be called on a server");
	}

	private void onHttpMessageComplete() {
		assert !isClosed();
		if (inspector != null) inspector.onHttpResponseComplete(this);
		if (IWebSocket.ENABLED && isWebSocket()) return;

		if ((flags & KEEP_ALIVE) != 0 && server.keepAliveTimeoutMillis != 0) {
			request = nullify(request, HttpMessage::recycle);
			switchPool(server.poolKeepAlive);

			if (socket.isReadAvailable()) {
				socket.read().whenResult(buf -> readBuf = readBuf == null ? buf : ByteBufPool.append(readBuf, buf));
			}

			read();
		} else {
			close();
		}
	}

	private void writeException(Exception e) {
		server.formatHttpError(e).whenComplete(this::writeHttpResponse, this::closeEx);
	}

	@Override
	protected void onClosed() {
		if (pool != server.poolServing) {
			request = nullify(request, HttpMessage::recycle);
			readBuf = nullify(readBuf, ByteBuf::recycle);
			stashedBufs = nullify(stashedBufs, Recyclable::recycle);
		}
		if (inspector != null) inspector.onDisconnect(this);
		//noinspection ConstantConditions
		pool.removeNode(this);
		//noinspection AssertWithSideEffects,ConstantConditions
		assert (pool = null) == null;
		server.onConnectionClosed();
		writeBuf = Utils.nullify(writeBuf, ByteBuf::recycle);
	}

	@Override
	public String toString() {
		return
			"HttpServerConnection{" +
			"pool=" + getCurrentPool() +
			", remoteAddress=" + remoteAddress +
			',' + super.toString() +
			'}';
	}
}
