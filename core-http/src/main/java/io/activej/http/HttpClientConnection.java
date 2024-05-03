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

import io.activej.async.exception.AsyncCloseException;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.ApplicationSettings;
import io.activej.common.recycle.Recyclable;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.http.HttpClient.Inspector;
import io.activej.http.stream.BufsConsumerGzipInflater;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;

import static io.activej.bytebuf.ByteBufStrings.SP;
import static io.activej.common.Utils.nullify;
import static io.activej.csp.supplier.ChannelSuppliers.concat;
import static io.activej.http.HttpHeaderValue.ofBytes;
import static io.activej.http.HttpHeaders.CONNECTION;
import static io.activej.http.HttpHeaders.SEC_WEBSOCKET_KEY;
import static io.activej.http.HttpMessage.MUST_LOAD_BODY;
import static io.activej.http.HttpUtils.*;
import static io.activej.http.WebSocketConstants.HANDSHAKE_FAILED;
import static io.activej.http.WebSocketConstants.REGULAR_CLOSE;
import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * <p>
 * This class is responsible for sending and receiving HTTP requests.
 * It's made so that one instance of it corresponds to one networking socket.
 * That's why instances of those classes are all stored in one of three pools in their
 * respective {@link HttpClient} instance.
 * </p>
 * <p>
 * Those pools are: <code>poolKeepAlive</code>, <code>poolReading</code>, and <code>poolWriting</code>.
 * </p>
 * Path between those pools that any connection takes can be represented as a state machine,
 * described as a GraphViz graph below.
 * Nodes with (null) descriptor mean that the connection is not in any pool.
 * <pre>
 * digraph {
 *     label="Single HttpConnection state machine"
 *     rankdir="LR"
 *
 *     "open(null)"
 *     "closed(null)"
 *     "reading"
 *     "writing"
 *     "keep-alive"
 *     "taken(null)"
 *
 *     "writing" -> "closed(null)" [color="#ff8080", style=dashed, label="value reset/write timeout"]
 *     "reading" -> "closed(null)" [color="#ff8080", style=dashed, label="value reset/read timeout"]
 *     "keep-alive" -> "closed(null)" [color="#ff8080", style=dashed, label="value reset"]
 *     "taken(null)" -> "closed(null)" [color="#ff8080", style=dashed, label="value reset"]
 *
 *     "open(null)" -> "writing" [label="send request"]
 *     "writing" -> "reading"
 *     "reading" -> "closed(null)" [label="received response\n(no keep-alive)"]
 *     "reading" -> "keep-alive" [label="received response"]
 *     "keep-alive" -> "taken(null)" [label="reuse connection"]
 *     "taken(null)" -> "writing" [label="send request"]
 *     "keep-alive" -> "closed(null)" [label="expiration"]
 *
 *     { rank=same; "open(null)", "closed(null)" }
 *     { rank=same; "reading", "writing", "keep-alive" }
 * }
 * </pre>
 */
public final class HttpClientConnection extends AbstractHttpConnection {
	private static final boolean DETAILED_ERROR_MESSAGES = ApplicationSettings.getBoolean(HttpClientConnection.class, "detailedErrorMessages", false);

	private @Nullable SettablePromise<HttpResponse> promise;
	private final HttpClient client;
	private final @Nullable Inspector inspector;

	@Nullable HttpResponse response;
	final InetSocketAddress remoteAddress;
	@Nullable HttpClientConnection addressPrev;
	HttpClientConnection addressNext;

	HttpClientConnection(Reactor reactor, HttpClient client, ITcpSocket socket, InetSocketAddress remoteAddress) {
		super(reactor, socket, client.maxBodySize);
		this.client = client;
		this.inspector = client.inspector;
		this.remoteAddress = remoteAddress;
	}

	@Override
	public PoolLabel getCurrentPool() {
		if (pool == client.poolKeepAlive) return PoolLabel.KEEP_ALIVE;
		if (pool == client.poolReadWrite) return PoolLabel.READ_WRITE;
		return PoolLabel.NONE;
	}

	public InetSocketAddress getRemoteAddress() {
		return remoteAddress;
	}

	@Override
	protected void readMessage() throws MalformedHttpException {
		readStartLine();
	}

	@Override
	protected void onClosedWithError(Exception e) {
		if (inspector != null) inspector.onHttpError(this, e);
		if (promise != null) {
			SettablePromise<HttpResponse> promise = this.promise;
			this.promise = null;
			promise.setException(e);
		}
	}

	@Override
	protected void onMalformedHttpException(MalformedHttpException e) {
		if (inspector != null) {
			inspector.onMalformedHttpResponse(this, e, readBuf == null ? new byte[0] : readBuf.getArray());
		}

		closeEx(e);
	}

	@Override
	protected void onStartLine(byte[] line, int pos, int limit) throws MalformedHttpException {
		//noinspection PointlessArithmeticExpression
		boolean http1x = line[pos + 0] == 'H' && line[pos + 1] == 'T' && line[pos + 2] == 'T' && line[pos + 3] == 'P' && line[pos + 4] == '/' && line[pos + 5] == '1';
		boolean http11 = line[pos + 6] == '.' && line[pos + 7] == '1' && line[pos + 8] == SP;

		if (!http1x) {
			if (!DETAILED_ERROR_MESSAGES) throw new MalformedHttpException("Invalid response");
			throw new MalformedHttpException("Invalid response. First line: " + new String(line, 0, limit, ISO_8859_1));
		}

		int i = pos + 9;
		HttpVersion version = HttpVersion.HTTP_1_1;
		if (http11) {
			flags |= KEEP_ALIVE;
		} else if (line[6] == '.' && line[7] == '0' && line[8] == SP) {
			version = HttpVersion.HTTP_1_0;
		} else if (line[6] == SP) {
			version = HttpVersion.HTTP_1_0;
			i += 7 - 9;
		} else {
			if (!DETAILED_ERROR_MESSAGES) throw new MalformedHttpException("Invalid response");
			throw new MalformedHttpException("Invalid response. First line: " + new String(line, 0, limit, ISO_8859_1));
		}

		int statusCode = decodePositiveInt(line, i, 3);
		if (!(statusCode >= 100 && statusCode < 600)) {
			throw new MalformedHttpException("Invalid HTTP Status Code " + statusCode);
		}
		response = new HttpResponse(version, statusCode, this);
		response.maxBodySize = maxBodySize;
		/*
		  RFC 2616, section 4.4
		  1.Any response message which "MUST NOT" include a message-body (such as the 1xx, 204, and 304 responses and any response to a HEAD request) is always
		  terminated by the first empty line after the header fields, regardless of the entity-header fields present in the message.
		 */
		if (statusCode < 200 || statusCode == 204 || statusCode == 304) {
			// Reset Content-Length for the case keep-alive connection
			contentLength = 0L;
		}
	}

	@Override
	protected void onHeader(HttpHeader header, byte[] array, int off, int len) throws MalformedHttpException {
		assert response != null;
		if (response.headers.size() >= MAX_HEADERS) throw new MalformedHttpException("Too many headers");
		response.headers.add(header, ofBytes(array, off, len));
	}

	@Override
	protected void onHeadersReceived(@Nullable ByteBuf body, @Nullable ChannelSupplier<ByteBuf> bodySupplier) {
		assert !isClosed();

		HttpResponse response = this.response;
		//noinspection ConstantConditions
		response.flags |= MUST_LOAD_BODY;
		response.body = body;
		response.bodyStream = bodySupplier == null ? null : sanitize(bodySupplier);
		if (IWebSocket.ENABLED && isWebSocket()) {
			if (!processWebSocketResponse(body)) return;
		}
		if (inspector != null) inspector.onHttpResponse(response);

		SettablePromise<HttpResponse> promise = this.promise;
		this.promise = null;
		//noinspection ConstantConditions
		promise.set(response);
	}

	@SuppressWarnings("ConstantConditions")
	private boolean processWebSocketResponse(@Nullable ByteBuf body) {
		if (response.getCode() == 101) {
			assert body != null && body.readRemaining() == 0;
			response.bodyStream = sanitize(concat(ChannelSuppliers.ofValue(detachReadBuf()), ChannelSuppliers.ofSocket(socket)));
			return true;
		} else {
			closeEx(HANDSHAKE_FAILED);
			return false;
		}
	}

	@Override
	protected void onBodyReceived() {
		assert !isClosed();
		flags |= BODY_RECEIVED;
		if (response != null && (flags & BODY_SENT) != 0) {
			onHttpMessageComplete();
		}
	}

	@Override
	protected void onBodySent() {
		assert !isClosed();
		flags |= BODY_SENT;
		if (response != null && (flags & BODY_RECEIVED) != 0) {
			onHttpMessageComplete();
		}
	}

	@Override
	protected void onNoContentLength() {
		ChannelZeroBuffer<ByteBuf> buffer = new ChannelZeroBuffer<>();
		ChannelSupplier<ByteBuf> supplier = ChannelSuppliers.concat(ChannelSuppliers.ofValue(detachReadBuf()), buffer.getSupplier());
		Promise<Void> inflaterFinished = Promise.complete();
		if ((flags & GZIPPED) != 0) {
			BufsConsumerGzipInflater gzipInflater = BufsConsumerGzipInflater.create();
			supplier = supplier.transformWith(gzipInflater);
			inflaterFinished = gzipInflater.getProcessCompletion();
		}
		onHeadersReceived(null, supplier);
		ChannelSuppliers.ofAsyncSupplier(socket::read, socket)
			.streamTo(buffer.getConsumer())
			.both(inflaterFinished)
			.subscribe(($, e) -> {
				if (isClosed()) return;
				if (e == null) {
					onBodyReceived();
				} else {
					closeEx(translateToHttpException(e));
				}
			});
	}

	Promise<IWebSocket> sendWebSocketRequest(HttpRequest request) {
		assert !isClosed();
		SettablePromise<HttpResponse> promise = new SettablePromise<>();
		this.promise = promise;
		(pool = client.poolReadWrite).addLastNode(this);
		poolTimestamp = reactor.currentTimeMillis();
		flags |= WEB_SOCKET;

		byte[] encodedKey = generateWebSocketKey();
		request.headers.add(SEC_WEBSOCKET_KEY, ofBytes(encodedKey));

		ChannelZeroBuffer<ByteBuf> buffer = new ChannelZeroBuffer<>();
		request.bodyStream = sanitize(buffer.getSupplier());

		writeHttpMessageAsStream(null, request);

		if (!isClosed()) {
			readHttpResponse();
		}
		return promise
			.<IWebSocket>thenCallback((res, cb) -> {
				assert res.getCode() == 101;
				if (isAnswerInvalid(res, encodedKey)) {
					closeEx(HANDSHAKE_FAILED);
					cb.setException(HANDSHAKE_FAILED);
					return;
				}
				int maxWebSocketMessageSize = client.maxWebSocketMessageSize;

				WebSocketFramesToBufs encoder = WebSocketFramesToBufs.create(true);
				WebSocketBufsToFrames decoder = WebSocketBufsToFrames.create(
					maxWebSocketMessageSize,
					encoder::sendPong,
					ByteBuf::recycle,
					false);

				bindWebSocketTransformers(encoder, decoder);

				cb.set(new WebSocket(
					request,
					res,
					res.takeBodyStream().transformWith(decoder),
					buffer.getConsumer().transformWith(encoder),
					decoder::onProtocolError,
					maxWebSocketMessageSize
				));
			})
			.whenException(e -> closeEx(translateToHttpException(e)));
	}

	private void bindWebSocketTransformers(WebSocketFramesToBufs encoder, WebSocketBufsToFrames decoder) {
		encoder.getCloseSentPromise()
			.then(decoder::getCloseReceivedPromise)
			.whenException(this::closeWebSocketConnection)
			.whenResult(this::closeWebSocketConnection);

		decoder.getProcessCompletion()
			.subscribe(($, e) -> {
				if (isClosed()) return;
				if (e == null) {
					encoder.sendCloseFrame(REGULAR_CLOSE);
				} else {
					encoder.closeEx(e);
				}
			});
	}

	private void readHttpResponse() {
		/*
			as per RFC 7230, section 3.3.3,
			if no Content-Length header is set, client should read body until a server closes the connection
		*/
		contentLength = UNSET_CONTENT_LENGTH;
		if (readBuf == null) {
			read();
		} else {
			reactor.post(() -> {
				if (isClosed()) return;
				read();
			});
		}
	}

	private void onHttpMessageComplete() {
		if (inspector != null) inspector.onRequestComplete(response, this);
		if (IWebSocket.ENABLED && isWebSocket()) return;

		//noinspection ConstantConditions
		response.recycle();
		response = null;
		if (stashedBufs != null) {
			stashedBufs.recycle();
			stashedBufs = null;
		}
		if (readBuf != null && !readBuf.canRead()) {
			readBuf.recycle();
			readBuf = null;
		}

		if ((flags & KEEP_ALIVE) != 0 &&
			client.keepAliveTimeoutMillis != 0 &&
			((flags & CHUNKED) != 0 || contentLength != UNSET_CONTENT_LENGTH)
		) {
			flags = 0;
			socket.read()
				.subscribe((buf, e) -> {
					if (e == null) {
						if (buf != null) {
							buf.recycle();
							closeEx(new HttpException("Unexpected read data"));
						} else {
							close();
						}
					} else {
						closeEx(translateToHttpException(e));
					}
				});
			if (isClosed()) return;
			client.returnToKeepAlivePool(this);
		} else {
			close();
		}
	}

	/**
	 * Sends the request, recycles it and closes connection in case of timeout
	 *
	 * @param request request for sending
	 */
	Promise<HttpResponse> send(HttpRequest request) {
		assert !isClosed();
		SettablePromise<HttpResponse> promise = new SettablePromise<>();
		this.promise = promise;
		(pool = client.poolReadWrite).addLastNode(this);
		poolTimestamp = reactor.currentTimeMillis();

		tryAddHeader(request, CONNECTION, () -> {
			if (++numberOfRequests >= client.maxKeepAliveRequests &&
				client.maxKeepAliveRequests != 0 || client.keepAliveTimeoutMillis == 0) {
				return CONNECTION_CLOSE_HEADER;
			}
			return CONNECTION_KEEP_ALIVE_HEADER;
		});

		ByteBuf buf = renderHttpMessage(request);
		if (buf != null) {
			writeBuf(buf);
		} else {
			writeHttpMessageAsStream(null, request);
		}
		if (!isClosed()) {
			readHttpResponse();
		}
		return promise;
	}

	/**
	 * After closing this connection it removes it from its connections cache and recycles
	 * Http response.
	 */
	@Override
	protected void onClosed() {
		if (inspector != null) inspector.onDisconnect(this);
		if (promise != null) {
			SettablePromise<HttpResponse> promise = this.promise;
			this.promise = null;
			promise.setException(new AsyncCloseException("Connection closed"));
		}
		if (pool == client.poolKeepAlive) {
			AddressLinkedList addresses = client.addresses.get(remoteAddress);
			addresses.removeNode(this);
			if (addresses.isEmpty()) {
				client.addresses.remove(remoteAddress);
			}
		}

		// pool will be null if socket was closed by the value just before connection.send() invocation
		// (e.g. if connection was in open(null) or taken(null) states)
		//noinspection ConstantConditions
		pool.removeNode(this);

		client.handleShutdown();
		response = nullify(response, HttpMessage::recycle);
		readBuf = nullify(readBuf, ByteBuf::recycle);
		stashedBufs = nullify(stashedBufs, Recyclable::recycle);
	}

	@Override
	public String toString() {
		return
			"HttpClientConnection{" +
			"pool=" + getCurrentPool() +
			", promise=" + promise +
			", response=" + response +
			", httpClient=" + client +
			//				", lastRequestUrl='" + (request.getFullUrl() == null ? "" : request.getFullUrl()) + '\'' +
			", remoteAddress=" + remoteAddress +
			',' + super.toString() +
			'}';
	}
}
