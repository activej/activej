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

package io.activej.net.socket.tcp;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.ApplicationSettings;
import io.activej.common.recycle.Recyclers;
import io.activej.eventloop.net.CloseWithoutNotifyException;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.*;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.CLOSED;

/**
 * This is an SSL proxy around {@link AsyncTcpSocket}.
 * <p>
 * It allows SSL connections using Java {@link SSLEngine}.
 */
public final class AsyncTcpSocketSsl implements AsyncTcpSocket {
	public static final boolean ERROR_ON_CLOSE_WITHOUT_NOTIFY = ApplicationSettings.getBoolean(AsyncTcpSocketSsl.class, "errorOnCloseWithoutNotify", false);
	private final SSLEngine engine;
	private final Executor executor;
	private final AsyncTcpSocket upstream;

	private ByteBuf net2engine = ByteBuf.empty();
	private ByteBuf engine2app = ByteBuf.empty();
	private ByteBuf app2engine = ByteBuf.empty();
	private boolean shouldReturnEndOfStream;

	@Nullable
	private SettablePromise<ByteBuf> read;
	@Nullable
	private SettablePromise<Void> write;
	@Nullable
	private Promise<Void> pendingUpstreamWrite;

	public static AsyncTcpSocketSsl wrapClientSocket(AsyncTcpSocket asyncTcpSocket,
			String host, int port,
			SSLContext sslContext, Executor executor) {
		SSLEngine sslEngine = sslContext.createSSLEngine(host, port);
		sslEngine.setUseClientMode(true);
		return create(asyncTcpSocket, sslEngine, executor);
	}

	public static AsyncTcpSocketSsl wrapClientSocket(AsyncTcpSocket asyncTcpSocket,
			SSLContext sslContext, Executor executor) {
		SSLEngine sslEngine = sslContext.createSSLEngine();
		sslEngine.setUseClientMode(true);
		return create(asyncTcpSocket, sslEngine, executor);
	}

	public static AsyncTcpSocketSsl wrapServerSocket(AsyncTcpSocket asyncTcpSocket,
			SSLContext sslContext, Executor executor) {
		SSLEngine sslEngine = sslContext.createSSLEngine();
		sslEngine.setUseClientMode(false);
		return create(asyncTcpSocket, sslEngine, executor);
	}

	private AsyncTcpSocketSsl(AsyncTcpSocket asyncTcpSocket, SSLEngine engine, Executor executor) {
		this.engine = engine;
		this.executor = executor;
		this.upstream = asyncTcpSocket;
		startHandShake();
	}

	public static AsyncTcpSocketSsl create(AsyncTcpSocket asyncTcpSocket,
			SSLEngine engine, Executor executor) {
		return new AsyncTcpSocketSsl(asyncTcpSocket, engine, executor);
	}

	@NotNull
	private <T> Promise<T> sanitize(T value, @Nullable Throwable e) {
		if (e == null) {
			return Promise.of(value);
		} else {
			closeEx(e);
			return Promise.ofException(e);
		}
	}

	@NotNull
	@Override
	public Promise<ByteBuf> read() {
		read = null;
		if (shouldReturnEndOfStream) {
			shouldReturnEndOfStream = false;
			return Promise.of(null);
		}
		if (isClosed()) return Promise.ofException(CLOSE_EXCEPTION);
		if (engine2app.canRead()) {
			ByteBuf readBuf = engine2app;
			engine2app = ByteBuf.empty();
			return Promise.of(readBuf);
		}
		SettablePromise<ByteBuf> read = new SettablePromise<>();
		this.read = read;
		sync();
		return read;
	}

	@NotNull
	@Override
	public Promise<Void> write(@Nullable ByteBuf buf) {
		if (isClosed()) {
			if (buf != null) {
				buf.recycle();
			}
			return Promise.ofException(CLOSE_EXCEPTION);
		}
		if (buf == null) {
			throw new UnsupportedOperationException("SSL cannot work in half-duplex mode");
		}
		if (!buf.canRead()) {
			buf.recycle();
			return write == null ? Promise.complete() : write;
		}
		app2engine = ByteBufPool.append(app2engine, buf);
		if (write != null) return write;
		SettablePromise<Void> write = new SettablePromise<>();
		this.write = write;
		sync();
		return write;
	}

	private void doRead() {
		upstream.read()
				.thenEx(this::sanitize)
				.whenResult(buf -> {
					if (isClosed()) {
						assert pendingUpstreamWrite != null;
						Recyclers.recycle(buf);
						return;
					}
					if (buf != null) {
						net2engine = ByteBufPool.append(net2engine, buf);
						sync();
					} else {
						if (engine.isInboundDone()) return;
						try {
							engine.closeInbound();
						} catch (SSLException e) {
							if (!ERROR_ON_CLOSE_WITHOUT_NOTIFY && read != null) {
								SettablePromise<ByteBuf> read = this.read;
								this.read = null;
								read.set(null);
							}
							closeEx(new CloseWithoutNotifyException(AsyncTcpSocketSsl.class, "Peer closed without sending close_notify", e));
						}
					}
				});
	}

	private void doWrite(ByteBuf dstBuf) {
		Promise<Void> writePromise = upstream.write(dstBuf);
		if (this.pendingUpstreamWrite != null) {
			return;
		}
		if (!writePromise.isComplete()) {
			this.pendingUpstreamWrite = writePromise;
		}
		writePromise
				.thenEx(this::sanitize)
				.whenComplete(() -> this.pendingUpstreamWrite = null)
				.whenResult(() -> {
					if (isClosed()) {
						return;
					}
					if (engine.isOutboundDone()) {
						close();
						return;
					}
					if (!app2engine.canRead() && engine.getHandshakeStatus() == NOT_HANDSHAKING && write != null) {
						SettablePromise<Void> write = this.write;
						this.write = null;
						write.set(null);
					}
				});
	}

	private SSLEngineResult tryToUnwrap() throws SSLException {
		ByteBuf dstBuf = ByteBufPool.allocate(engine.getSession().getPacketBufferSize());
		ByteBuffer srcBuffer = net2engine.toReadByteBuffer();
		ByteBuffer dstBuffer = dstBuf.toWriteByteBuffer();

		SSLEngineResult result;
		try {
			result = engine.unwrap(srcBuffer, dstBuffer);
		} catch (SSLException e) {
			dstBuf.recycle();
			throw e;
		} catch (RuntimeException e) {
			// https://bugs.openjdk.java.net/browse/JDK-8072452
			dstBuf.recycle();
			throw new SSLException(e);
		}

		net2engine.ofReadByteBuffer(srcBuffer);
		net2engine = recycleIfEmpty(net2engine);

		dstBuf.ofWriteByteBuffer(dstBuffer);
		if (!isClosed() && dstBuf.canRead()) {
			engine2app = ByteBufPool.append(engine2app, dstBuf);
		} else {
			dstBuf.recycle();
		}

		return result;
	}

	private SSLEngineResult tryToWrap() throws SSLException {
		ByteBuf dstBuf = ByteBufPool.allocate(engine.getSession().getPacketBufferSize());
		ByteBuffer srcBuffer = app2engine.toReadByteBuffer();
		ByteBuffer dstBuffer = dstBuf.toWriteByteBuffer();

		SSLEngineResult result;
		try {
			result = engine.wrap(srcBuffer, dstBuffer);
		} catch (SSLException e) {
			dstBuf.recycle();
			throw e;
		} catch (RuntimeException e) {
			// https://bugs.openjdk.java.net/browse/JDK-8072452
			dstBuf.recycle();
			throw new SSLException(e);
		}

		app2engine.ofReadByteBuffer(srcBuffer);
		app2engine = recycleIfEmpty(app2engine);

		dstBuf.ofWriteByteBuffer(dstBuffer);
		if (dstBuf.canRead()) {
			doWrite(dstBuf);
		} else {
			dstBuf.recycle();
		}
		return result;
	}

	/**
	 * This method is used for handling handshake routine as well as sending close_notify message to recipient
	 */
	private void doHandshake() throws SSLException {
		SSLEngineResult result = null;
		while (!isClosed()) {
			if (result != null && result.getStatus() == CLOSED) {
				close();
				return;
			}

			HandshakeStatus handshakeStatus = engine.getHandshakeStatus();
			if (handshakeStatus == NEED_WRAP) {
				result = tryToWrap();
			} else if (handshakeStatus == NEED_UNWRAP) {
				result = tryToUnwrap();
				if (result.getStatus() == BUFFER_UNDERFLOW) {
					doRead();
					return;
				}
			} else if (handshakeStatus == NEED_TASK) {
				executeTasks();
				return;
			} else {
				doSync();
				return;
			}
		}
	}

	private void executeTasks() {
		while (!isClosed()) {
			Runnable task = engine.getDelegatedTask();
			if (task == null) break;
			Promise.ofBlockingRunnable(executor, task::run)
					.whenResult(() -> {
						if (isClosed()) return;
						try {
							doHandshake();
						} catch (SSLException e) {
							closeEx(e);
						}
					});
		}
	}

	private void sync() {
		try {
			doSync();
		} catch (SSLException e) {
			closeEx(e);
		}
	}

	private void doSync() throws SSLException {
		if (isClosed()) return;
		SSLEngineResult result = null;
		HandshakeStatus handshakeStatus = engine.getHandshakeStatus();

		if (handshakeStatus != NOT_HANDSHAKING) {
			doHandshake();
			return;
		}

		// write data to net
		if (app2engine.canRead()) {
			do {
				result = tryToWrap();
			}
			while (!isClosed() && app2engine.canRead() && (result.bytesConsumed() != 0 || result.bytesProduced() != 0));
		}

		if (isClosed()) {
			return;
		}

		// read data from net
		if (net2engine.canRead()) {
			do {
				result = tryToUnwrap();
			} while (net2engine.canRead() && (result.bytesConsumed() != 0 || result.bytesProduced() != 0));

			// peer sent close_notify
			if (result.getStatus() == CLOSED) {
				shouldReturnEndOfStream = true;
			}

			if (read != null && engine2app.canRead()) {
				SettablePromise<ByteBuf> read = this.read;
				this.read = null;
				ByteBuf readBuf = engine2app;
				engine2app = ByteBuf.empty();
				read.set(readBuf);
			}
		}

		if (result != null && result.getStatus() == CLOSED) {
			close();
			return;
		}

		if (!isClosed() && (read != null || !engine2app.canRead())) {
			doRead();
		}
	}

	private static ByteBuf recycleIfEmpty(ByteBuf buf) {
		if (buf.canRead())
			return buf;
		buf.recycle();
		return ByteBuf.empty();
	}

	private void startHandShake() {
		try {
			engine.beginHandshake();
			sync();
		} catch (SSLException e) {
			closeEx(e);
		}
	}

	private void tryCloseOutbound() {
		if (!engine.isOutboundDone()) {
			engine.closeOutbound();
			try {
				while (!engine.isOutboundDone()) {
					SSLEngineResult result = tryToWrap();
					if (result.getStatus() == CLOSED) {
						break;
					}
				}
			} catch (SSLException ignored) {
			}
		}
	}

	@Override
	public void closeEx(@NotNull Throwable e) {
		if (isClosed()) return;
		Recyclers.recycle(net2engine);
		Recyclers.recycle(engine2app);
		net2engine = engine2app = null;
		tryCloseOutbound();
		// app2Engine is recycled later as it is used while sending close notify messages
		Recyclers.recycle(app2engine);
		app2engine = null;
		if (pendingUpstreamWrite != null) {
			pendingUpstreamWrite.whenResult(() -> upstream.closeEx(e));
		} else {
			upstream.closeEx(e);
		}
		if (write != null) {
			write.setException(e);
			write = null;
		}
		if (read != null) {
			if (shouldReturnEndOfStream) {
				shouldReturnEndOfStream = false;
				read.set(null);
			} else {
				read.setException(e);
			}
			read = null;
		}
	}

	@Override
	public boolean isClosed() {
		return net2engine == null;
	}

}
