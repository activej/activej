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
import io.activej.common.exception.StacklessException;
import io.activej.common.exception.parse.ParseException;
import io.activej.common.exception.parse.UnknownFormatException;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.ChannelSuppliers;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncHttpClient.Inspector;
import io.activej.http.stream.BufsConsumerGzipInflater;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;

import static io.activej.bytebuf.ByteBufStrings.SP;
import static io.activej.bytebuf.ByteBufStrings.decodePositiveInt;
import static io.activej.csp.ChannelSuppliers.concat;
import static io.activej.http.HttpHeaders.CONNECTION;
import static io.activej.http.HttpHeaders.SEC_WEBSOCKET_KEY;
import static io.activej.http.HttpMessage.MUST_LOAD_BODY;
import static io.activej.http.HttpUtils.generateWebSocketKey;
import static io.activej.http.HttpUtils.isAnswerInvalid;
import static io.activej.http.WebSocketConstants.HANDSHAKE_FAILED;
import static io.activej.http.WebSocketConstants.REGULAR_CLOSE;
import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * <p>
 * This class is responsible for sending and receiving HTTP requests.
 * It's made so that one instance of it corresponds to one networking socket.
 * That's why instances of those classes are all stored in one of three pools in their
 * respective {@link AsyncHttpClient} instance.
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
	public static final ParseException INVALID_RESPONSE = new UnknownFormatException(HttpClientConnection.class, "Invalid response");
	public static final StacklessException CONNECTION_CLOSED = new StacklessException(HttpClientConnection.class, "Connection closed");
	public static final StacklessException NOT_ACCEPTED_RESPONSE = new StacklessException(HttpClientConnection.class, "Response was not accepted");

	static final HttpHeaderValue CONNECTION_UPGRADE_HEADER = HttpHeaderValue.of("upgrade");
	static final HttpHeaderValue UPGRADE_WEBSOCKET_HEADER = HttpHeaderValue.of("websocket");

	@Nullable
	private SettablePromise<HttpResponse> promise;
	@Nullable
	private HttpResponse response;
	private final AsyncHttpClient client;
	@Nullable
	private final Inspector inspector;

	final InetSocketAddress remoteAddress;
	@Nullable HttpClientConnection addressPrev;
	HttpClientConnection addressNext;

	HttpClientConnection(Eventloop eventloop, AsyncHttpClient client, AsyncTcpSocket asyncTcpSocket, InetSocketAddress remoteAddress) {
		super(eventloop, asyncTcpSocket, client.maxBodySize);
		this.client = client;
		this.inspector = client.inspector;
		this.remoteAddress = remoteAddress;
	}

	public PoolLabel getCurrentPool() {
		if (pool == client.poolKeepAlive) return PoolLabel.KEEP_ALIVE;
		if (pool == client.poolReadWrite) return PoolLabel.READ_WRITE;
		return PoolLabel.NONE;
	}

	public InetSocketAddress getRemoteAddress() {
		return remoteAddress;
	}

	@Override
	protected void onClosedWithError(@NotNull Throwable e) {
		if (inspector != null) inspector.onHttpError(this, e);
		if (promise != null) {
			SettablePromise<HttpResponse> promise = this.promise;
			this.promise = null;
			promise.setException(e);
		}
	}

	@Override
	protected void onStartLine(byte[] line, int limit) throws ParseException {
		boolean http1x = line[0] == 'H' && line[1] == 'T' && line[2] == 'T' && line[3] == 'P' && line[4] == '/' && line[5] == '1';
		boolean http11 = line[6] == '.' && line[7] == '1' && line[8] == SP;

		if (!http1x) throw INVALID_RESPONSE;

		int pos = 9;
		HttpVersion version = HttpVersion.HTTP_1_1;
		if (http11) {
			flags |= KEEP_ALIVE;
		} else if (line[6] == '.' && line[7] == '0' && line[8] == SP) {
			version = HttpVersion.HTTP_1_0;
		} else if (line[6] == SP) {
			version = HttpVersion.HTTP_1_0;
			pos = 7;
		} else {
			throw new ParseException(HttpClientConnection.class, "Invalid response: " + new String(line, 0, limit, ISO_8859_1));
		}

		int statusCode = decodePositiveInt(line, pos, 3);
		if (!(statusCode >= 100 && statusCode < 600)) {
			throw new UnknownFormatException(HttpClientConnection.class, "Invalid HTTP Status Code " + statusCode);
		}
		response = new HttpResponse(version, statusCode);
		response.maxBodySize = maxBodySize;
		/*
		  RFC 2616, section 4.4
		  1.Any response message which "MUST NOT" include a message-body (such as the 1xx, 204, and 304 responses and any response to a HEAD request) is always
		  terminated by the first empty line after the header fields, regardless of the entity-header fields present in the message.
		 */
		if (statusCode < 200 || statusCode == 204 || statusCode == 304) {
			// Reset Content-Length for the case keep-alive connection
			contentLength = 0;
		}
	}

	@Override
	protected void onHeaderBuf(ByteBuf buf) {
		//noinspection ConstantConditions
		response.addHeaderBuf(buf);
	}

	@Override
	protected void onHeader(HttpHeader header, byte[] array, int off, int len) throws ParseException {
		assert response != null;
		if (response.headers.size() >= MAX_HEADERS) throw TOO_MANY_HEADERS;
		response.addHeader(header, array, off, len);
	}

	@Override
	protected void onHeadersReceived(@Nullable ByteBuf body, @Nullable ChannelSupplier<ByteBuf> bodySupplier) {
		assert !isClosed();

		HttpResponse response = this.response;
		//noinspection ConstantConditions
		response.flags |= MUST_LOAD_BODY;
		response.body = body;
		response.bodyStream = bodySupplier;
		if (isWebSocket()) {
			if (!processWebSocketResponse(body)) return;
		}
		if (inspector != null) inspector.onHttpResponse(this, response);

		SettablePromise<HttpResponse> promise = this.promise;
		this.promise = null;
		//noinspection ConstantConditions
		promise.set(response);
	}

	@SuppressWarnings("ConstantConditions")
	private boolean processWebSocketResponse(@Nullable ByteBuf body) {
		if (response.getCode() == 101) {
			assert body != null && body.readRemaining() == 0;
			response.bodyStream = concat(ChannelSupplier.of(readQueue.takeRemaining()), ChannelSupplier.ofSocket(socket));
			return true;
		} else {
			closeWithError(HANDSHAKE_FAILED);
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
		ChannelSupplier<ByteBuf> ofQueue = readQueue.hasRemaining() ? ChannelSupplier.of(readQueue.takeRemaining()) : ChannelSupplier.of();
		ChannelZeroBuffer<ByteBuf> buffer = new ChannelZeroBuffer<>();
		ChannelSupplier<ByteBuf> supplier = ChannelSuppliers.concat(ofQueue, buffer.getSupplier());
		Promise<Void> inflaterFinished = Promise.complete();
		if ((flags & GZIPPED) != 0) {
			BufsConsumerGzipInflater gzipInflater = BufsConsumerGzipInflater.create();
			supplier = supplier.transformWith(gzipInflater);
			inflaterFinished = gzipInflater.getProcessCompletion();
		}
		onHeadersReceived(null, supplier);
		ChannelSupplier.of(socket::read, socket)
				.streamTo(buffer.getConsumer())
				.both(inflaterFinished)
				.whenComplete(afterProcessCb);
	}

	@NotNull
	Promise<WebSocket> sendWebSocketRequest(HttpRequest request) {
		assert !isClosed();
		SettablePromise<HttpResponse> promise = new SettablePromise<>();
		this.promise = promise;
		(pool = client.poolReadWrite).addLastNode(this);
		poolTimestamp = eventloop.currentTimeMillis();
		flags |= WEB_SOCKET;

		byte[] encodedKey = generateWebSocketKey();
		request.addHeader(SEC_WEBSOCKET_KEY, encodedKey);

		ChannelZeroBuffer<ByteBuf> buffer = new ChannelZeroBuffer<>();
		request.setBodyStream(buffer.getSupplier());

		writeHttpMessageAsStream(request);
		request.recycle();

		if (!isClosed()) {
			readHttpResponse();
		}
		return promise
				.then(res -> {
					assert res.getCode() == 101;
					if (isAnswerInvalid(res, encodedKey)) {
						closeWithError(HANDSHAKE_FAILED);
						return Promise.ofException(HANDSHAKE_FAILED);
					}
					int maxWebSocketMessageSize = client.maxWebSocketMessageSize;

					WebSocketFramesToBufs encoder = WebSocketFramesToBufs.create(true);
					WebSocketBufsToFrames decoder = WebSocketBufsToFrames.create(
							maxWebSocketMessageSize,
							encoder::sendPong,
							ByteBuf::recycle,
							false);

					bindWebSocketTransformers(encoder, decoder);

					return Promise.of((WebSocket) new WebSocketImpl(
							request,
							res,
							res.getBodyStream().transformWith(decoder),
							buffer.getConsumer().transformWith(encoder),
							decoder::onProtocolError,
							maxWebSocketMessageSize
					));
				})
				.whenException(this::closeWithError);
	}

	private void bindWebSocketTransformers(WebSocketFramesToBufs encoder, WebSocketBufsToFrames decoder) {
		encoder.getCloseSentPromise()
				.then(decoder::getCloseReceivedPromise)
				.whenException(this::closeWebSocketConnection)
				.whenResult(this::closeWebSocketConnection);

		decoder.getProcessCompletion()
				.whenComplete(($, e) -> {
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
		if (readQueue.isEmpty()) {
			tryReadHttpMessage();
		} else {
			eventloop.post(() -> {
				if (isClosed()) return;
				tryReadHttpMessage();
			});
		}
	}

	private void onHttpMessageComplete() {
		if (isWebSocket()) return;

		assert response != null;
		response.recycle();
		response = null;

		if ((flags & KEEP_ALIVE) != 0 && client.keepAliveTimeoutMillis != 0 && contentLength != UNSET_CONTENT_LENGTH) {
			flags = 0;
			socket.read()
					.whenComplete((buf, e) -> {
						if (e == null) {
							if (buf != null) {
								buf.recycle();
								closeWithError(UNEXPECTED_READ);
							} else {
								close();
							}
						} else {
							closeWithError(e);
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
		poolTimestamp = eventloop.currentTimeMillis();
		HttpHeaderValue connectionHeader = CONNECTION_KEEP_ALIVE_HEADER;
		if (++numberOfKeepAliveRequests >= client.maxKeepAliveRequests &&
				client.maxKeepAliveRequests != 0 || client.keepAliveTimeoutMillis == 0) {
			connectionHeader = CONNECTION_CLOSE_HEADER;
		}
		request.addHeader(CONNECTION, connectionHeader);
		ByteBuf buf = renderHttpMessage(request);
		if (buf != null) {
			writeBuf(buf);
		} else {
			writeHttpMessageAsStream(request);
		}
		request.recycle();
		if (!isClosed()) {
			readHttpResponse();
		}
		return promise;
	}

	private void tryReadHttpMessage() {
		try {
			readHttpMessage();
		} catch (ParseException e) {
			closeWithError(e);
		}
	}

	/**
	 * After closing this connection it removes it from its connections cache and recycles
	 * Http response.
	 */
	@Override
	protected void onClosed() {
		if (promise != null) {
			if (inspector != null) inspector.onHttpError(this, CONNECTION_CLOSED);
			SettablePromise<HttpResponse> promise = this.promise;
			this.promise = null;
			promise.setException(CONNECTION_CLOSED);
		}
		if (pool == client.poolKeepAlive) {
			AddressLinkedList addresses = client.addresses.get(remoteAddress);
			addresses.removeNode(this);
			if (addresses.isEmpty()) {
				client.addresses.remove(remoteAddress);
			}
		}

		// pool will be null if socket was closed by the value just before connection.send() invocation
		// (eg. if connection was in open(null) or taken(null) states)
		//noinspection ConstantConditions
		pool.removeNode(this);

		client.onConnectionClosed();
		if (response != null) {
			response.recycle();
			response = null;
		}
	}

	@Override
	public String toString() {
		return "HttpClientConnection{" +
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
