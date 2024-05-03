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

import io.activej.async.callback.Callback;
import io.activej.async.process.AsyncCloseable;
import io.activej.async.process.ReactiveProcess;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.ApplicationSettings;
import io.activej.common.MemSize;
import io.activej.common.recycle.Recyclable;
import io.activej.csp.ChannelOutput;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.supplier.AbstractChannelSupplier;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.http.stream.*;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.http.HttpHeaderValue.ofBytes;
import static io.activej.http.HttpHeaderValue.ofDecimal;
import static io.activej.http.HttpHeaders.*;
import static io.activej.http.HttpUtils.translateToHttpException;
import static io.activej.http.HttpUtils.trimAndDecodePositiveLong;
import static java.lang.Math.max;

@SuppressWarnings({"WeakerAccess", "PointlessBitwiseExpression"})
public abstract class AbstractHttpConnection extends AbstractReactive {
	public static final MemSize MAX_HEADER_LINE_SIZE = MemSize.of(ApplicationSettings.getInt(HttpMessage.class, "maxHeaderLineSize", MemSize.kilobytes(8).toInt())); // http://stackoverflow.com/questions/686217/maximum-on-http-header-values
	public static final int MAX_HEADER_LINE_SIZE_BYTES = MAX_HEADER_LINE_SIZE.toInt(); // http://stackoverflow.com/questions/686217/maximum-on-http-header-values
	public static final int MAX_HEADERS = ApplicationSettings.getInt(HttpMessage.class, "maxHeaders", 100); // http://httpd.apache.org/docs/2.2/mod/core.html#limitrequestfields

	protected static final HttpHeaderValue CONNECTION_KEEP_ALIVE_HEADER = HttpHeaderValue.ofBytes(encodeAscii("keep-alive"));
	protected static final HttpHeaderValue CONNECTION_CLOSE_HEADER = HttpHeaderValue.ofBytes(encodeAscii("close"));
	protected static final long UNSET_CONTENT_LENGTH = -1L;

	protected static final byte[] CONNECTION_KEEP_ALIVE = encodeAscii("keep-alive");
	protected static final byte[] TRANSFER_ENCODING_CHUNKED = encodeAscii("chunked");
	protected static final byte[] CONTENT_ENCODING_GZIP = encodeAscii("gzip");

	protected static final byte[] UPGRADE_HEADER = encodeAscii("upgrade");
	protected static final byte[] UPGRADE_WEBSOCKET = encodeAscii("websocket");
	protected static final byte[] WEB_SOCKET_VERSION = encodeAscii("13");

	protected final ITcpSocket socket;
	protected final int maxBodySize;

	protected static final byte KEEP_ALIVE = 1 << 0;
	protected static final byte GZIPPED = 1 << 1;
	protected static final byte CHUNKED = 1 << 2;
	protected static final byte WEB_SOCKET = 1 << 3;
	protected static final byte BODY_RECEIVED = 1 << 4;
	protected static final byte BODY_SENT = 1 << 5;
	protected static final byte READING_MESSAGES = 1 << 6;
	protected static final byte CLOSED = (byte) (1 << 7);

	@MagicConstant(flags = {KEEP_ALIVE, GZIPPED, CHUNKED, WEB_SOCKET, BODY_RECEIVED, BODY_SENT, READING_MESSAGES, CLOSED})
	protected byte flags = 0;

	protected ByteBuf readBuf;

	protected @Nullable ConnectionsLinkedList pool;
	protected @Nullable AbstractHttpConnection prev;
	protected @Nullable AbstractHttpConnection next;
	protected long poolTimestamp;

	protected int numberOfRequests;

	protected long contentLength;

	protected @Nullable Exception closeException;

	private @Nullable Object userData;

	protected Recyclable stashedBufs;

	protected final ReadConsumer readMessageConsumer = new ReadConsumer() {
		@Override
		protected void thenRun() throws MalformedHttpException {
			readMessage();
		}
	};

	protected final ReadConsumer readHeadersConsumer = new ReadConsumer() {
		@Override
		protected void thenRun() throws MalformedHttpException {
			readHeaders(readBuf.head());
		}
	};

	/**
	 * Creates a new instance of AbstractHttpConnection
	 *
	 * @param reactor     reactor which will handle its I/O operations
	 * @param maxBodySize - maximum size of message body
	 */
	protected AbstractHttpConnection(Reactor reactor, ITcpSocket socket, int maxBodySize) {
		super(reactor);
		this.socket = socket;
		this.maxBodySize = maxBodySize;
	}

	protected abstract void onStartLine(byte[] line, int pos, int limit) throws MalformedHttpException;

	protected abstract void onHeader(HttpHeader header, byte[] array, int off, int len) throws MalformedHttpException;

	protected abstract void onHeadersReceived(@Nullable ByteBuf body, @Nullable ChannelSupplier<ByteBuf> bodySupplier);

	protected abstract void onBodyReceived();

	protected abstract void onBodySent();

	protected abstract void onNoContentLength(); // TODO - remove

	protected abstract void onClosed();

	protected abstract void onClosedWithError(Exception e);

	protected abstract void onMalformedHttpException(MalformedHttpException e);

	protected abstract PoolLabel getCurrentPool();

	public final boolean isClosed() {
		return flags < 0;
	}

	public boolean isKeepAlive() {
		return (flags & KEEP_ALIVE) != 0;
	}

	public boolean isGzipped() {
		return (flags & GZIPPED) != 0;
	}

	public boolean isChunked() {
		return (flags & CHUNKED) != 0;
	}

	public boolean isBodyReceived() {
		return (flags & BODY_RECEIVED) != 0;
	}

	public boolean isBodySent() {
		return (flags & BODY_SENT) != 0;
	}

	public boolean isWebSocket() {
		return (flags & WEB_SOCKET) != 0;
	}

	/**
	 * Sets an arbitrary object as a user-defined context for connection
	 * <p>
	 * It may be used e.g. by HTTP inspector for collecting statistics per connection.
	 */
	public void setUserData(@Nullable Object userData) {
		this.userData = userData;
	}

	public Duration getPoolTimestamp() {
		return Duration.ofMillis(poolTimestamp);
	}

	public @Nullable Object getUserData() {
		return userData;
	}

	public MemSize getContentLength() {
		return MemSize.bytes(contentLength);
	}

	public MemSize getMaxBodySize() {
		return MemSize.bytes(maxBodySize);
	}

	public int getNumberOfRequests() {
		return numberOfRequests;
	}

	protected void closeWebSocketConnection(Exception e) {
		if (e instanceof WebSocketException) {
			close();
		} else {
			closeEx(translateToHttpException(e));
		}
	}

	protected final void close() {
		if (isClosed()) return;
		flags |= CLOSED;
		onClosed();
		socket.close();
	}

	protected final void closeEx(Exception e) {
		if (isClosed()) return;
		flags |= CLOSED;
		onClosedWithError(e);
		onClosed();
		socket.closeEx(e);
		closeException = e;
	}

	protected void read() {
		if (readBuf == null) {
			socket.read().subscribe(readMessageConsumer);
			return;
		}
		try {
			readMessage();
		} catch (MalformedHttpException e) {
			onMalformedHttpException(e);
		}
	}

	protected abstract void readMessage() throws MalformedHttpException;

	protected final void readStartLine() throws MalformedHttpException {
		byte[] array = readBuf.array();
		int head = readBuf.head();
		int tail = readBuf.tail();
		for (int p = head; p < tail; p++) {
			if (array[p] == LF) {
				onStartLine(array, head, p + 1);
				readHeaders(p + 1);
				return;
			}
		}
		if (readBuf.readRemaining() > MAX_HEADER_LINE_SIZE_BYTES)
			throw new MalformedHttpException("Header line exceeds max header size");
		socket.read().subscribe(readMessageConsumer);
	}

	private void readHeaders(int from) throws MalformedHttpException {
		byte[] array = readBuf.array();
		int offset = from;
		int tail = readBuf.tail();

		assert !isClosed();
		while (offset < tail) {
			int i;
			for (i = offset; i < tail; i++) {
				if (array[i] != LF) continue;

				// check next byte to see if this is multiline header(CRLF + 1*(SP|HT)) rfc2616#2.2
				if (i <= offset + 1 || (i + 1 < tail && (array[i + 1] != SP && array[i + 1] != HT))) {
					// fast processing path
					int limit = (i - 1 >= offset && array[i - 1] == CR) ? i - 1 : i;
					if (limit != offset) {
						readHeader(array, offset, limit);
						offset = i + 1;
						continue;
					} else {
						readBuf.head(i + 1);
						readBody();
						return;
					}
				}
				break;
			}

			if (i == tail && tail - offset <= 1) {
				break; // cannot determine if this is multiline header or not, need more data
			}

			int headerLen = scanHeader(max(offset, i - 1), array, offset, tail);

			if (headerLen == -1) {
				break;
			}
			if (headerLen != 0) {
				readHeader(array, offset, offset + headerLen);
			}
			offset += headerLen;
			if (array[offset] == CR) offset++;
			if (array[offset] == LF) offset++;
			if (headerLen == 0) {
				readBuf.head(offset);
				readBody();
				return;
			}
		}
		readBuf.head(offset);
		if (readBuf.readRemaining() > MAX_HEADER_LINE_SIZE_BYTES)
			throw new MalformedHttpException("Header line exceeds max header size");
		socket.read().subscribe(readHeadersConsumer);
	}

	private int scanHeader(int from, byte[] array, int head, int tail) throws MalformedHttpException {
		int i = from;
		while (true) {
			for (; i < tail; i++) {
				byte b = array[i];
				if (b == CR || b == LF) break;
			}
			if (i >= tail) return -1;
			byte b = array[i];
			if (b == CR) {
				if (++i >= tail) return -1;
				b = array[i];
				if (b != LF) throw new MalformedHttpException("Invalid CRLF");
				if (i - head == 1) {
					return 0;
				} else {
					if (++i >= tail) return -1;
					b = array[i];
					if (b == SP || b == HT) {
						array[i - 2] = SP;
						array[i - 1] = SP;
						continue;
					}
					return i - 2 - head;
				}
			} else {
				assert b == LF;
				if (i - head == 0) {
					return 0;
				} else {
					if (++i >= tail) return -1;
					b = array[i];
					if (b == SP || b == HT) {
						array[i - 1] = SP;
						continue;
					}
					return i - 1 - head;
				}
			}
		}
	}

	private void readHeader(byte[] array, int off, int limit) throws MalformedHttpException {
		int pos = off;
		int hashCodeCI = 0;
		while (pos < limit) {
			byte b = array[pos];
			if (b == ':')
				break;
			hashCodeCI += (b | 0x20);
			pos++;
		}
		if (pos == limit) throw new MalformedHttpException("Header name is absent");
		HttpHeader header = HttpHeaders.of(hashCodeCI, array, off, pos - off);
		pos++;

		// RFC 2616, section 19.3 Tolerant Applications
		while (pos < limit && (array[pos] == SP || array[pos] == HT)) {
			pos++;
		}

		int len = limit - pos;
		if (header == CONTENT_LENGTH) {
			contentLength = trimAndDecodePositiveLong(array, pos, len);
		} else if (header == CONNECTION) {
			flags = (byte) (
				(flags & ~KEEP_ALIVE) |
				(equalsLowerCaseAscii(CONNECTION_KEEP_ALIVE, array, pos, len) ? KEEP_ALIVE : 0));
		} else if (IWebSocket.ENABLED && header == HttpHeaders.UPGRADE) {
			flags |= equalsLowerCaseAscii(UPGRADE_WEBSOCKET, array, pos, len) ? WEB_SOCKET : 0;
		} else if (header == TRANSFER_ENCODING) {
			flags |= equalsLowerCaseAscii(TRANSFER_ENCODING_CHUNKED, array, pos, len) ? CHUNKED : 0;
		} else if (header == CONTENT_ENCODING) {
			flags |= equalsLowerCaseAscii(CONTENT_ENCODING_GZIP, array, pos, len) ? GZIPPED : 0;
		}

		onHeader(header, array, pos, len);
	}

	private void readBody() {
		assert !isClosed();
		if ((flags & CHUNKED) == 0) {
			if (contentLength == 0) {
				onHeadersReceived(ByteBuf.empty(), null);
				if (isClosed()) return;
				onBodyReceived();
				return;
			}
			if (contentLength == UNSET_CONTENT_LENGTH) {
				onNoContentLength();
				return;
			}
			if (((flags & GZIPPED) == 0) && readBuf.readRemaining() >= contentLength) {
				ByteBuf body;
				if (readBuf.readRemaining() == contentLength) {
					body = readBuf;
					readBuf = null;
				} else {
					body = readBuf.slice((int) contentLength);
					readBuf.moveHead((int) contentLength);
				}
				onHeadersReceived(body, null);
				if (isClosed()) return;
				onBodyReceived();
				return;
			}
		}

		ByteBuf readBuf = detachReadBuf();
		ByteBufs readBufs = new ByteBufs();
		readBufs.add(readBuf);
		BinaryChannelSupplier encodedStream = BinaryChannelSupplier.ofProvidedBufs(
			readBufs,
			() -> socket.read()
				.thenCallback(
					(buf, cb) -> {
						if (buf == null) {
							cb.setException(new MalformedHttpException("Incomplete HTTP message"));
							return;
						}
						readBufs.add(buf);
						cb.set(null);
					},
					(e, cb) -> {
						e = translateToHttpException(e);
						closeEx(e);
						cb.setException(e);
					}),
			Promise::complete,
			AsyncCloseable.of(e -> {
				Exception httpException = translateToHttpException(e);
				if (e instanceof MalformedHttpException) {
					onMalformedHttpException((MalformedHttpException) e);
				} else {
					closeEx(httpException);
				}
			}));

		ChannelOutput<ByteBuf> bodyStream;
		ReactiveProcess process;

		if ((flags & CHUNKED) == 0) {
			BufsConsumerDelimiter decoder = BufsConsumerDelimiter.create(contentLength);
			process = decoder;
			encodedStream.bindTo(decoder.getInput());
			bodyStream = decoder.getOutput();
		} else {
			BufsConsumerChunkedDecoder decoder = BufsConsumerChunkedDecoder.create();
			process = decoder;
			encodedStream.bindTo(decoder.getInput());
			bodyStream = decoder.getOutput();
		}

		if ((flags & GZIPPED) != 0) {
			BufsConsumerGzipInflater decoder = BufsConsumerGzipInflater.create();
			process = decoder;
			bodyStream.bindTo(decoder.getInput());
			bodyStream = decoder.getOutput();
		}

		ChannelSupplier<ByteBuf> supplier = bodyStream.getSupplier()
			.withEndOfStream(eos -> eos
				.mapException(HttpUtils::translateToHttpException)); // process gets started here and can cause connection closing

		if (isClosed()) return;

		onHeadersReceived(null, supplier);

		process.getProcessCompletion()
			.subscribe(($, e) -> {
				if (isClosed()) return;
				if (e == null) {
					assert this.readBuf == null;
					this.readBuf = readBufs.hasRemaining() ? readBufs.takeRemaining() : null;
					onBodyReceived();
				} else {
					closeEx(translateToHttpException(e));
				}
			});
	}

	static ByteBuf renderHttpMessage(HttpMessage httpMessage) {
		if (httpMessage.body != null) {
			ByteBuf body = httpMessage.body;
			httpMessage.body = null;
			if ((httpMessage.flags & HttpMessage.USE_GZIP) == 0) {
				httpMessage.headers.addIfAbsent(CONTENT_LENGTH, () -> ofDecimal(body.readRemaining()));
				ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize() + body.readRemaining());
				httpMessage.writeTo(buf);
				buf.put(body);
				body.recycle();
				return buf;
			} else {
				ByteBuf gzippedBody = GzipProcessorUtils.toGzip(body);
				httpMessage.headers.addIfAbsent(CONTENT_ENCODING, () -> ofBytes(CONTENT_ENCODING_GZIP));
				httpMessage.headers.addIfAbsent(CONTENT_LENGTH, () -> ofDecimal(gzippedBody.readRemaining()));
				ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize() + gzippedBody.readRemaining());
				httpMessage.writeTo(buf);
				buf.put(gzippedBody);
				gzippedBody.recycle();
				return buf;
			}
		}

		if (httpMessage.bodyStream == null) {
			if (httpMessage.isContentLengthExpected()) {
				httpMessage.headers.addIfAbsent(CONTENT_LENGTH, () -> ofDecimal(0));
			}
			ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize());
			httpMessage.writeTo(buf);
			return buf;
		}

		return null;
	}

	protected void writeHttpMessageAsStream(@Nullable ByteBuf writeBuf, HttpMessage httpMessage) {
		ChannelSupplier<ByteBuf> bodyStream = httpMessage.bodyStream;
		assert bodyStream != null;
		httpMessage.bodyStream = null;

		if (!IWebSocket.ENABLED || !isWebSocket()) {
			if ((httpMessage.flags & HttpMessage.USE_GZIP) != 0) {
				httpMessage.headers.addIfAbsent(CONTENT_ENCODING, () -> ofBytes(CONTENT_ENCODING_GZIP));
				BufsConsumerGzipDeflater deflater = BufsConsumerGzipDeflater.create();
				bodyStream.bindTo(deflater.getInput());
				bodyStream = deflater.getOutput().getSupplier();
			}

			if (httpMessage.headers.get(CONTENT_LENGTH) == null) {
				httpMessage.headers.addIfAbsent(TRANSFER_ENCODING, () -> ofBytes(TRANSFER_ENCODING_CHUNKED));
				BufsConsumerChunkedEncoder chunker = BufsConsumerChunkedEncoder.create();
				bodyStream.bindTo(chunker.getInput());
				bodyStream = chunker.getOutput().getSupplier();
			}
		}

		ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize());
		httpMessage.writeTo(buf);

		writeStream(ChannelSuppliers.concat(writeBuf != null ? ChannelSuppliers.ofValues(writeBuf, buf) : ChannelSuppliers.ofValue(buf), bodyStream));
	}

	protected void writeBuf(ByteBuf buf) {
		socket.write(buf)
			.subscribe(($, e) -> {
				if (isClosed()) return;
				if (e == null) {
					onBodySent();
				} else {
					closeEx(translateToHttpException(e));
				}
			});
	}

	private void writeStream(ChannelSupplier<ByteBuf> supplier) {
		supplier.streamTo(ChannelConsumers.ofAsyncConsumer(
				buf -> socket.write(buf)
					.whenException(e -> closeEx(translateToHttpException(e))),
				AsyncCloseable.of(e -> closeEx(translateToHttpException(e)))))
			.whenResult(this::onBodySent);
	}

	protected void switchPool(ConnectionsLinkedList newPool) {
		//noinspection ConstantConditions
		pool.removeNode(this);
		(pool = newPool).addLastNode(this);
		poolTimestamp = reactor.currentTimeMillis();
	}

	public abstract class ReadConsumer implements Callback<ByteBuf> {
		@Override
		public void accept(ByteBuf buf, Exception e) {
			assert !isClosed() || e != null;
			if (e == null) {
				if (buf != null) {
					ByteBuf readBuf = AbstractHttpConnection.this.readBuf;
					if (readBuf == null) {
						AbstractHttpConnection.this.readBuf = buf;
					} else {
						if (readBuf.writeRemaining() >= buf.readRemaining()) {
							readBuf.put(buf);
							buf.recycle();
						} else {
							stashBuf(readBuf);
							if (!readBuf.canRead()) {
								AbstractHttpConnection.this.readBuf = buf;
							} else {
								ByteBuf newBuf = ByteBufPool.allocate(readBuf.readRemaining() + buf.readRemaining());
								newBuf.put(readBuf);
								newBuf.put(buf);
								buf.recycle();
								AbstractHttpConnection.this.readBuf = newBuf;
							}
						}
					}
					try {
						thenRun();
					} catch (MalformedHttpException e1) {
						onMalformedHttpException(e1);
					}
				} else {
					if (getCurrentPool() == PoolLabel.KEEP_ALIVE && readBuf == null) {
						close();
					} else {
						onMalformedHttpException(new MalformedHttpException("Unexpected end of data"));
					}
				}
			} else {
				closeEx(translateToHttpException(e));
			}
		}

		protected abstract void thenRun() throws MalformedHttpException;
	}

	protected final void stashBuf(ByteBuf buf) {
		if (stashedBufs == null) {
			stashedBufs = buf;
		} else {
			Recyclable prev = this.stashedBufs;
			this.stashedBufs = () -> {
				prev.recycle();
				buf.recycle();
			};
		}
	}

	protected final ByteBuf detachReadBuf() {
		ByteBuf readBuf = this.readBuf;
		this.readBuf = null;
		stashBuf(readBuf);
		readBuf.addRef();
		return readBuf;
	}

	protected final ChannelSupplier<ByteBuf> sanitize(ChannelSupplier<ByteBuf> bodyStream) {
		return new AbstractChannelSupplier<>(bodyStream) {
			@Override
			protected Promise<ByteBuf> doGet() {
				if (closeException != null) {
					closeEx(closeException);
					return Promise.ofException(closeException);
				}
				return bodyStream.get()
					.thenCallback((result, cb) -> {
						if (closeException != null) {
							if (result != null) {
								result.recycle();
							}
							closeEx(closeException);
							cb.setException(closeException);
							return;
						}
						cb.set(result);
					});
			}
		};
	}

	@Override
	public String toString() {
		return
			", socket=" + socket +
			", readBuf=" + readBuf +
			", closed=" + isClosed() +
			", keepAlive=" + isKeepAlive() +
			", gzipped=" + isGzipped() +
			", chunked=" + isChunked() +
			", webSocket=" + (IWebSocket.ENABLED && isWebSocket()) +
			", contentLengthRemaining=" + contentLength +
			", poolTimestamp=" + poolTimestamp;
	}

}
