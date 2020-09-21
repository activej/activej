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
import io.activej.async.process.AsyncProcess;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.ApplicationSettings;
import io.activej.common.MemSize;
import io.activej.common.exception.AsyncTimeoutException;
import io.activej.common.exception.parse.ParseException;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelOutput;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.ChannelSuppliers;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.http.stream.*;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.Promise;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.time.Duration;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.http.HttpHeaderValue.ofBytes;
import static io.activej.http.HttpHeaderValue.ofDecimal;
import static io.activej.http.HttpHeaders.*;
import static io.activej.http.HttpUtils.trimAndDecodePositiveInt;
import static io.activej.http.PoolLabel.NONE;
import static java.lang.Math.max;

@SuppressWarnings({"WeakerAccess", "PointlessBitwiseExpression"})
public abstract class AbstractHttpConnection {
	public static final AsyncTimeoutException READ_TIMEOUT_ERROR = new AsyncTimeoutException(AbstractHttpConnection.class, "Read timeout");
	public static final AsyncTimeoutException WRITE_TIMEOUT_ERROR = new AsyncTimeoutException(AbstractHttpConnection.class, "Write timeout");
	public static final ParseException HEADER_NAME_ABSENT = new ParseException(AbstractHttpConnection.class, "Header name is absent");
	public static final ParseException TOO_LONG_HEADER = new ParseException(AbstractHttpConnection.class, "Header line exceeds max header size");
	public static final ParseException TOO_MANY_HEADERS = new ParseException(AbstractHttpConnection.class, "Too many headers");
	public static final ParseException INCOMPLETE_MESSAGE = new ParseException(AbstractHttpConnection.class, "Incomplete HTTP message");
	public static final ParseException UNEXPECTED_READ = new ParseException(AbstractHttpConnection.class, "Unexpected read data");
	public static final ParseException INVALID_CRLF = new ParseException(AbstractHttpConnection.class, "Invalid CRLF");

	public static final MemSize MAX_HEADER_LINE_SIZE = MemSize.of(ApplicationSettings.getInt(HttpMessage.class, "maxHeaderLineSize", MemSize.kilobytes(8).toInt())); // http://stackoverflow.com/questions/686217/maximum-on-http-header-values
	public static final int MAX_HEADER_LINE_SIZE_BYTES = MAX_HEADER_LINE_SIZE.toInt(); // http://stackoverflow.com/questions/686217/maximum-on-http-header-values
	public static final int MAX_HEADERS = ApplicationSettings.getInt(HttpMessage.class, "maxHeaders", 100); // http://httpd.apache.org/docs/2.2/mod/core.html#limitrequestfields

	protected static final HttpHeaderValue CONNECTION_KEEP_ALIVE_HEADER = HttpHeaderValue.of("keep-alive");
	protected static final HttpHeaderValue CONNECTION_CLOSE_HEADER = HttpHeaderValue.of("close");
	protected static final int UNSET_CONTENT_LENGTH = -1;

	private static final byte[] CONNECTION_KEEP_ALIVE = encodeAscii("keep-alive");
	private static final byte[] TRANSFER_ENCODING_CHUNKED = encodeAscii("chunked");
	private static final byte[] CONTENT_ENCODING_GZIP = encodeAscii("gzip");
	private static final byte[] EMPTY_HEADER = new byte[0];

	protected static final byte[] UPGRADE_WEBSOCKET = encodeAscii("websocket");
	protected static final byte[] WEB_SOCKET_VERSION = encodeAscii("13");

	protected final Eventloop eventloop;

	protected final AsyncTcpSocket socket;
	protected final ByteBufQueue readQueue = new ByteBufQueue();
	protected final InetSocketAddress remoteAddress;
	protected final int maxBodySize;

	protected static final byte KEEP_ALIVE = 1 << 0;
	protected static final byte GZIPPED = 1 << 1;
	protected static final byte CHUNKED = 1 << 2;
	protected static final byte WEB_SOCKET = 1 << 3;
	protected static final byte BODY_RECEIVED = 1 << 4;
	protected static final byte BODY_SENT = 1 << 5;
	protected static final byte CLOSED = (byte) (1 << 7);

	@MagicConstant(flags = {KEEP_ALIVE, GZIPPED, CHUNKED, WEB_SOCKET, BODY_RECEIVED, BODY_SENT, CLOSED})
	protected byte flags = 0;

	@Nullable
	protected ConnectionsLinkedList pool;
	@Nullable
	protected AbstractHttpConnection prev;
	@Nullable
	protected AbstractHttpConnection next;
	protected long poolTimestamp;

	protected int numberOfKeepAliveRequests;

	protected int contentLength;

	protected final ReadConsumer startLineConsumer = new ReadConsumer() {
		@Override
		public void thenRun() throws ParseException {
			readStartLine();
		}
	};

	protected final ReadConsumer headersConsumer = new ReadConsumer() {
		@Override
		public void thenRun() throws ParseException {
			readHeaders();
		}
	};

	protected final Callback<Void> afterProcessCb = ($, e) -> {
		if (isClosed()) return;
		if (e == null) {
			onBodyReceived();
		} else {
			closeWithError(e);
		}
	};

	/**
	 * Creates a new instance of AbstractHttpConnection
	 *
	 * @param eventloop   eventloop which will handle its I/O operations
	 * @param maxBodySize - maximum size of message body
	 */
	public AbstractHttpConnection(Eventloop eventloop, AsyncTcpSocket socket, int maxBodySize) {
		this.eventloop = eventloop;
		this.socket = socket;
		this.remoteAddress = socket.getRemoteAddress();
		this.maxBodySize = maxBodySize;
	}

	protected abstract void onStartLine(byte[] line, int limit) throws ParseException;

	protected abstract void onHeaderBuf(ByteBuf buf);

	protected abstract void onHeader(HttpHeader header, byte[] array, int off, int len) throws ParseException;

	protected abstract void onHeadersReceived(@Nullable ByteBuf body, @Nullable ChannelSupplier<ByteBuf> bodySupplier);

	protected abstract void onBodyReceived();

	protected abstract void onBodySent();

	protected abstract void onNoContentLength();

	protected abstract void onClosed();

	protected abstract void onClosedWithError(@NotNull Throwable e);

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

	public Duration getPoolTimestamp() {
		return Duration.ofMillis(poolTimestamp);
	}

	public MemSize getContentLength() {
		return MemSize.bytes(contentLength);
	}

	public InetSocketAddress getRemoteAddress() {
		return remoteAddress;
	}

	public MemSize getMaxBodySize() {
		return MemSize.bytes(maxBodySize);
	}

	public int getNumberOfKeepAliveRequests() {
		return numberOfKeepAliveRequests;
	}

	public PoolLabel getCurrentPool() {
		return pool == null ? NONE : pool.getLabel();
	}

	protected void closeWebSocketConnection(Throwable e) {
		if (e instanceof WebSocketException) {
			close();
		} else {
			closeWithError(e);
		}
	}

	protected final void close() {
		if (isClosed()) return;
		flags |= CLOSED;
		onClosed();
		socket.close();
		readQueue.recycle();
	}

	protected final void closeWithError(@NotNull Throwable e) {
		if (isClosed()) return;
		flags |= CLOSED;
		onClosedWithError(e);
		onClosed();
		socket.close();
		readQueue.recycle();
	}

	protected final void readHttpMessage() throws ParseException {
		readStartLine();
	}

	private void readStartLine() throws ParseException {
		int size = 1;
		for (int i = 0; i < readQueue.remainingBufs(); i++) {
			ByteBuf buf = readQueue.peekBuf(i);
			for (int p = buf.head(); p < buf.tail(); p++) {
				if (buf.at(p) == LF) {
					size += p - buf.head();
					if (i == 0 && buf.head() == 0 && size >= 10) {
						onStartLine(buf.array(), size);
						readQueue.skip(size);
					} else {
						ByteBuf line = ByteBufPool.allocate(max(10, size)); // allocate at least 16 bytes
						readQueue.drainTo(line, size);
						try {
							onStartLine(line.array(), size);
						} finally {
							line.recycle();
						}
					}
					readHeaders();
					return;
				}
			}
			size += buf.readRemaining();
		}
		if (readQueue.hasRemainingBytes(MAX_HEADER_LINE_SIZE_BYTES)) throw TOO_LONG_HEADER;
		socket.read().whenComplete(startLineConsumer);
	}

	private void readHeaders() throws ParseException {
		assert !isClosed();
		while (readQueue.hasRemaining()) {
			ByteBuf buf = readQueue.peekBuf(0);
			byte[] array = buf.array();
			int head = buf.head();
			int tail = buf.tail();
			int i;
			for (i = head; i < tail; i++) {
				if (array[i] != LF) continue;

				// check next byte to see if this is multiline header(CRLF + 1*(SP|HT)) rfc2616#2.2
				if (i <= head + 1 || (i + 1 < tail && (array[i + 1] != SP && array[i + 1] != HT))) {
					// fast processing path
					int limit = (i - 1 >= head && array[i - 1] == CR) ? i - 1 : i;
					if (limit != head) {
						processHeaderLine(array, head, limit);
						readQueue.skip(i - head + 1, this::onHeaderBuf);
						head = buf.head();
						continue;
					} else {
						onHeaderBuf(buf);
						readQueue.skip(i - head + 1);
						readBody();
						return;
					}
				}
				break;
			}

			if (i == tail && readQueue.remainingBufs() <= 1) {
				break; // cannot determine if this is multiline header or not, need more data
			}

			byte[] header = readHeaderEx(max(0, i - head - 1));
			if (header == null) break;
			if (header.length != 0) {
				processHeaderLine(header, 0, header.length);
			} else {
				readBody();
				return;
			}
		}

		if (readQueue.hasRemainingBytes(MAX_HEADER_LINE_SIZE_BYTES)) throw TOO_LONG_HEADER;
		socket.read().whenComplete(headersConsumer);
	}

	private byte[] readHeaderEx(int i) throws ParseException {
		int remainingBytes = readQueue.remainingBytes();
		while (true) {
			i = readQueue.scanBytes(i, b -> b == CR || b == LF);
			if (i >= remainingBytes) return null;
			byte b = readQueue.peekByte(i++);
			assert b == CR || b == LF;
			byte[] bytes;
			if (b == CR) {
				if (i >= remainingBytes) return null;
				b = readQueue.peekByte(i++);
				if (b != LF) throw INVALID_CRLF;
				if (i == 2) {
					bytes = EMPTY_HEADER;
				} else {
					if (i >= remainingBytes) return null;
					b = readQueue.peekByte(i);
					if (b == SP || b == HT) {
						readQueue.setByte(i - 2, SP);
						readQueue.setByte(i - 1, SP);
						continue;
					}
					bytes = new byte[i - 2];
				}
			} else {
				if (i == 1) {
					bytes = EMPTY_HEADER;
				} else {
					if (i >= remainingBytes) return null;
					b = readQueue.peekByte(i);
					if (b == SP || b == HT) {
						readQueue.setByte(i - 1, SP);
						continue;
					}
					bytes = new byte[i - 1];
				}
			}

			readQueue.drainTo(bytes, 0, bytes.length, this::onHeaderBuf);
			readQueue.skip(i - bytes.length, this::onHeaderBuf);
			return bytes;
		}
	}

	private void processHeaderLine(byte[] array, int off, int limit) throws ParseException {
		int pos = off;
		int hashCode = 1;
		while (pos < limit) {
			byte b = array[pos];
			if (b == ':')
				break;
			if (b >= 'A' && b <= 'Z')
				b += 'a' - 'A';
			hashCode = 31 * hashCode + b;
			pos++;
		}
		if (pos == limit) throw HEADER_NAME_ABSENT;
		HttpHeader header = HttpHeaders.of(array, off, pos - off, hashCode);
		pos++;

		// RFC 2616, section 19.3 Tolerant Applications
		while (pos < limit && (array[pos] == SP || array[pos] == HT)) {
			pos++;
		}

		int len = limit - pos;
		if (header == CONTENT_LENGTH) {
			contentLength = trimAndDecodePositiveInt(array, pos, len);
		} else if (header == CONNECTION) {
			flags = (byte) ((flags & ~KEEP_ALIVE) |
					(equalsLowerCaseAscii(CONNECTION_KEEP_ALIVE, array, pos, len) ? KEEP_ALIVE : 0));
		} else if (header == HttpHeaders.UPGRADE) {
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
			if (((flags & GZIPPED) == 0) && readQueue.hasRemainingBytes(contentLength)) {
				ByteBuf body = readQueue.takeExactSize(contentLength);
				onHeadersReceived(body, null);
				if (isClosed()) return;
				onBodyReceived();
				return;
			}
		}

		BinaryChannelSupplier encodedStream = BinaryChannelSupplier.ofProvidedQueue(
				readQueue,
				() -> socket.read()
						.thenEx((buf, e) -> {
							if (e == null) {
								if (buf != null) {
									readQueue.add(buf);
									return Promise.complete();
								} else {
									return Promise.ofException(INCOMPLETE_MESSAGE);
								}
							} else {
								closeWithError(e);
								return Promise.ofException(e);
							}
						}),
				Promise::complete,
				this::closeWithError);

		ChannelOutput<ByteBuf> bodyStream;
		AsyncProcess process;

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

		ChannelSupplier<ByteBuf> supplier = bodyStream.getSupplier(); // process gets started here and can cause connection closing

		if (isClosed()) return;

		onHeadersReceived(null, supplier);

		process.getProcessCompletion()
				.whenComplete(afterProcessCb);
	}

	static ByteBuf renderHttpMessage(HttpMessage httpMessage) {
		if (httpMessage.body != null) {
			ByteBuf body = httpMessage.body;
			httpMessage.body = null;
			if ((httpMessage.flags & HttpMessage.USE_GZIP) == 0) {
				httpMessage.addHeader(CONTENT_LENGTH, ofDecimal(body.readRemaining()));
				ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize() + body.readRemaining());
				httpMessage.writeTo(buf);
				buf.put(body);
				body.recycle();
				return buf;
			} else {
				ByteBuf gzippedBody = GzipProcessorUtils.toGzip(body);
				httpMessage.addHeader(CONTENT_ENCODING, ofBytes(CONTENT_ENCODING_GZIP));
				httpMessage.addHeader(CONTENT_LENGTH, ofDecimal(gzippedBody.readRemaining()));
				ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize() + gzippedBody.readRemaining());
				httpMessage.writeTo(buf);
				buf.put(gzippedBody);
				gzippedBody.recycle();
				return buf;
			}
		}

		if (httpMessage.bodyStream == null) {
			if (httpMessage.isContentLengthExpected()) {
				httpMessage.addHeader(CONTENT_LENGTH, ofDecimal(0));
			}
			ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize());
			httpMessage.writeTo(buf);
			return buf;
		}

		return null;
	}

	protected void writeHttpMessageAsStream(HttpMessage httpMessage) {
		ChannelSupplier<ByteBuf> bodyStream = httpMessage.bodyStream;
		assert bodyStream != null;
		httpMessage.bodyStream = null;

		if (!isWebSocket()) {
			if ((httpMessage.flags & HttpMessage.USE_GZIP) != 0) {
				httpMessage.addHeader(CONTENT_ENCODING, ofBytes(CONTENT_ENCODING_GZIP));
				BufsConsumerGzipDeflater deflater = BufsConsumerGzipDeflater.create();
				bodyStream.bindTo(deflater.getInput());
				bodyStream = deflater.getOutput().getSupplier();
			}

			if (httpMessage.headers.get(CONTENT_LENGTH) == null) {
				httpMessage.addHeader(TRANSFER_ENCODING, ofBytes(TRANSFER_ENCODING_CHUNKED));
				BufsConsumerChunkedEncoder chunker = BufsConsumerChunkedEncoder.create();
				bodyStream.bindTo(chunker.getInput());
				bodyStream = chunker.getOutput().getSupplier();
			}
		}

		ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize());
		httpMessage.writeTo(buf);

		writeStream(ChannelSuppliers.concat(ChannelSupplier.of(buf), bodyStream));
	}

	protected void writeBuf(ByteBuf buf) {
		socket.write(buf)
				.whenComplete(($, e) -> {
					if (isClosed()) return;
					if (e == null) {
						onBodySent();
					} else {
						closeWithError(e);
					}
				});
	}

	private void writeStream(ChannelSupplier<ByteBuf> supplier) {
		supplier.streamTo(ChannelConsumer.of(
				buf -> socket.write(buf)
						.whenException(this::closeWithError),
				this::closeWithError))
				.whenResult(this::onBodySent);
	}

	protected void switchPool(ConnectionsLinkedList newPool) {
		//noinspection ConstantConditions
		pool.removeNode(this);
		(pool = newPool).addLastNode(this);
		poolTimestamp = eventloop.currentTimeMillis();
	}

	private abstract class ReadConsumer implements Callback<ByteBuf> {
		@Override
		public void accept(ByteBuf buf, Throwable e) {
			assert !isClosed() || e != null;
			if (e == null) {
				if (buf != null) {
					readQueue.add(buf);
					try {
						thenRun();
					} catch (ParseException e1) {
						closeWithError(e1);
					}
				} else {
					close();
				}
			} else {
				closeWithError(e);
			}
		}

		public abstract void thenRun() throws ParseException;
	}

	@Override
	public String toString() {
		return ", socket=" + socket +
				", readQueue=" + readQueue +
				", closed=" + isClosed() +
				", keepAlive=" + ((flags & KEEP_ALIVE) != 0) +
				", isGzipped=" + ((flags & GZIPPED) != 0) +
				", isChunked=" + ((flags & CHUNKED) != 0) +
				", contentLengthRemaining=" + contentLength +
				", poolTimestamp=" + poolTimestamp;
	}

}
