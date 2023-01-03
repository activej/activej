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

import io.activej.async.exception.AsyncCloseException;
import io.activej.async.exception.AsyncTimeoutException;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.ApplicationSettings;
import io.activej.common.Checks;
import io.activej.common.inspector.AbstractInspector;
import io.activej.common.inspector.BaseInspector;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.EventStats;
import io.activej.jmx.stats.ExceptionStats;
import io.activej.jmx.stats.ValueStats;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.AbstractNioReactive;
import io.activej.reactor.net.SocketSettings;
import io.activej.reactor.nio.NioChannelEventHandler;
import io.activej.reactor.nio.NioReactor;
import io.activej.reactor.schedule.ScheduledRunnable;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static io.activej.common.Checks.checkState;
import static io.activej.common.MemSize.kilobytes;
import static io.activej.common.Utils.nullify;

@SuppressWarnings("WeakerAccess")
public final class ReactiveTcpSocket extends AbstractNioReactive implements TcpSocket, NioChannelEventHandler {
	private static final boolean CHECK = Checks.isEnabled(ReactiveTcpSocket.class);

	private static final int DEBUG_READ_OFFSET = ApplicationSettings.getInt(ReactiveTcpSocket.class, "debugReadOffset", 0);

	public static final int DEFAULT_READ_BUFFER_SIZE = ApplicationSettings.getMemSize(ReactiveTcpSocket.class, "readBufferSize", kilobytes(16)).toInt();
	public static final int NO_TIMEOUT = 0;

	private static final AtomicInteger CONNECTION_COUNT = new AtomicInteger(0);

	private final InetSocketAddress remoteAddress;

	private @Nullable SocketChannel channel;
	private @Nullable ByteBuf readBuf;
	private boolean readEndOfStream;
	private @Nullable ByteBuf writeBuf;
	private boolean writeEndOfStream;

	private @Nullable SettablePromise<ByteBuf> read;
	private @Nullable SettablePromise<Void> write;

	private SelectionKey key;
	private byte ops;

	private int readTimeout = NO_TIMEOUT;
	private int writeTimeout = NO_TIMEOUT;
	private int readBufferSize = DEFAULT_READ_BUFFER_SIZE;

	private @Nullable ScheduledRunnable scheduledReadTimeout;
	private @Nullable ScheduledRunnable scheduledWriteTimeout;

	private @Nullable Inspector inspector;

	private @Nullable Object userData;

	public interface Inspector extends BaseInspector<Inspector> {
		void onConnect(ReactiveTcpSocket socket);

		void onReadTimeout(ReactiveTcpSocket socket);

		void onRead(ReactiveTcpSocket socket, ByteBuf buf);

		void onReadEndOfStream(ReactiveTcpSocket socket);

		void onReadError(ReactiveTcpSocket socket, IOException e);

		void onWriteTimeout(ReactiveTcpSocket socket);

		void onWrite(ReactiveTcpSocket socket, ByteBuf buf, int bytes);

		void onWriteError(ReactiveTcpSocket socket, IOException e);

		void onDisconnect(ReactiveTcpSocket socket);
	}

	public static class JmxInspector extends AbstractInspector<Inspector> implements Inspector {
		public static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);

		private final EventStats connects = EventStats.create(SMOOTHING_WINDOW);
		private final ValueStats reads = ValueStats.create(SMOOTHING_WINDOW).withUnit("bytes").withRate();
		private final EventStats readEndOfStreams = EventStats.create(SMOOTHING_WINDOW);
		private final ExceptionStats readErrors = ExceptionStats.create();
		private final EventStats readTimeouts = EventStats.create(SMOOTHING_WINDOW);
		private final ValueStats writes = ValueStats.create(SMOOTHING_WINDOW).withUnit("bytes").withRate();
		private final ExceptionStats writeErrors = ExceptionStats.create();
		private final EventStats writeTimeouts = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats writeOverloaded = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats disconnects = EventStats.create(SMOOTHING_WINDOW);

		@Override
		public void onConnect(ReactiveTcpSocket socket) {
			connects.recordEvent();
		}

		@Override
		public void onReadTimeout(ReactiveTcpSocket socket) {
			readTimeouts.recordEvent();
		}

		@Override
		public void onRead(ReactiveTcpSocket socket, ByteBuf buf) {
			reads.recordValue(buf.readRemaining());
		}

		@Override
		public void onReadEndOfStream(ReactiveTcpSocket socket) {
			readEndOfStreams.recordEvent();
		}

		@Override
		public void onReadError(ReactiveTcpSocket socket, IOException e) {
			readErrors.recordException(e, socket.getRemoteAddress());
		}

		@Override
		public void onWriteTimeout(ReactiveTcpSocket socket) {
			writeTimeouts.recordEvent();
		}

		@Override
		public void onWrite(ReactiveTcpSocket socket, ByteBuf buf, int bytes) {
			writes.recordValue(bytes);
			if (buf.readRemaining() != bytes)
				writeOverloaded.recordEvent();
		}

		@Override
		public void onWriteError(ReactiveTcpSocket socket, IOException e) {
			writeErrors.recordException(e, socket.getRemoteAddress());
		}

		@Override
		public void onDisconnect(ReactiveTcpSocket socket) {
			disconnects.recordEvent();
		}

		@JmxAttribute
		public EventStats getReadTimeouts() {
			return readTimeouts;
		}

		@JmxAttribute
		public ValueStats getReads() {
			return reads;
		}

		@JmxAttribute
		public EventStats getReadEndOfStreams() {
			return readEndOfStreams;
		}

		@JmxAttribute
		public ExceptionStats getReadErrors() {
			return readErrors;
		}

		@JmxAttribute
		public EventStats getWriteTimeouts() {
			return writeTimeouts;
		}

		@JmxAttribute
		public ValueStats getWrites() {
			return writes;
		}

		@JmxAttribute
		public ExceptionStats getWriteErrors() {
			return writeErrors;
		}

		@JmxAttribute
		public EventStats getWriteOverloaded() {
			return writeOverloaded;
		}

		@JmxAttribute
		public EventStats getConnects() {
			return connects;
		}

		@JmxAttribute
		public EventStats getDisconnects() {
			return disconnects;
		}

		@JmxAttribute
		public long getActiveSockets() {
			return connects.getTotalCount() - disconnects.getTotalCount();
		}
	}

	private ReactiveTcpSocket(NioReactor reactor, @Nullable SocketChannel socketChannel, InetSocketAddress remoteAddress) {
		super(reactor);
		this.channel = socketChannel;
		this.remoteAddress = remoteAddress;
	}

	public static ReactiveTcpSocket wrapChannel(NioReactor reactor, SocketChannel socketChannel, InetSocketAddress remoteAddress, @Nullable SocketSettings socketSettings) throws IOException {
		ReactiveTcpSocket tcpSocket = new ReactiveTcpSocket(reactor, socketChannel, remoteAddress);
		if (socketSettings == null) return tcpSocket;
		socketSettings.applySettings(socketChannel);
		if (socketSettings.hasImplReadTimeout()) {
			tcpSocket.readTimeout = (int) socketSettings.getImplReadTimeoutMillis();
		}
		if (socketSettings.hasImplWriteTimeout()) {
			tcpSocket.writeTimeout = (int) socketSettings.getImplWriteTimeoutMillis();
		}
		if (socketSettings.hasReadBufferSize()) {
			tcpSocket.readBufferSize = socketSettings.getImplReadBufferSizeBytes();
		}
		return tcpSocket;
	}

	public static ReactiveTcpSocket wrapChannel(NioReactor reactor, SocketChannel socketChannel, @Nullable SocketSettings socketSettings) throws IOException {
		return wrapChannel(reactor, socketChannel, ((InetSocketAddress) socketChannel.getRemoteAddress()), socketSettings);
	}

	public static Promise<ReactiveTcpSocket> connect(NioReactor reactor, InetSocketAddress address) {
		return connect(reactor, address, null, null);
	}

	public static Promise<ReactiveTcpSocket> connect(NioReactor reactor, InetSocketAddress address, @Nullable Duration duration, @Nullable SocketSettings socketSettings) {
		return connect(reactor, address, duration == null ? 0 : duration.toMillis(), socketSettings);
	}

	public static Promise<ReactiveTcpSocket> connect(NioReactor reactor, InetSocketAddress address, long timeout, @Nullable SocketSettings socketSettings) {
		return Promise.<SocketChannel>ofCallback(cb -> reactor.connect(address, timeout, cb))
				.map(channel -> {
					try {
						return wrapChannel(reactor, channel, address, socketSettings);
					} catch (IOException e) {
						reactor.closeChannel(channel, null);
						throw e;
					}
				});
	}

	public void setInspector(@Nullable Inspector inspector) {
		this.inspector = inspector;
	}
	// endregion

	public static int getConnectionCount() {
		return CONNECTION_COUNT.get();
	}

	public InetSocketAddress getRemoteAddress() {
		return remoteAddress;
	}

	public @Nullable Object getUserData() {
		return userData;
	}

	/**
	 * Sets an arbitrary object as a user-defined context for this socket
	 * <p>
	 * It may be used e.g. by socket inspector for collecting statistics per socket.
	 */
	public void setUserData(@Nullable Object userData) {
		this.userData = userData;
	}

	// timeouts management
	private void scheduleReadTimeout() {
		assert scheduledReadTimeout == null && readTimeout != NO_TIMEOUT;
		scheduledReadTimeout = reactor.delayBackground(readTimeout, () -> {
			if (inspector != null) inspector.onReadTimeout(this);
			scheduledReadTimeout = null;
			closeEx(new AsyncTimeoutException("Timed out"));
		});
	}

	private void scheduleWriteTimeout() {
		assert scheduledWriteTimeout == null && writeTimeout != NO_TIMEOUT;
		scheduledWriteTimeout = reactor.delayBackground(writeTimeout, () -> {
			if (inspector != null) inspector.onWriteTimeout(this);
			scheduledWriteTimeout = null;
			closeEx(new AsyncTimeoutException("Timed out"));
		});
	}

	private void updateInterests() {
		assert !isClosed() && ops >= 0;
		byte newOps = (byte) (((readBuf == null && !readEndOfStream) ? SelectionKey.OP_READ : 0) | (writeBuf == null || writeEndOfStream ? 0 : SelectionKey.OP_WRITE));
		if (key == null) {
			ops = newOps;
			try {
				key = channel.register(reactor.ensureSelector(), ops, this);
				CONNECTION_COUNT.incrementAndGet();
			} catch (ClosedChannelException e) {
				closeEx(e);
			}
		} else {
			if (ops != newOps) {
				ops = newOps;
				key.interestOps(ops);
			}
		}
	}

	@Override
	public Promise<ByteBuf> read() {
		if (CHECK) checkState(inReactorThread());
		if (isClosed()) return Promise.ofException(new AsyncCloseException());
		read = null;
		if (readBuf != null || readEndOfStream) {
			ByteBuf readBuf = this.readBuf;
			this.readBuf = null;
			return Promise.of(readBuf);
		}
		SettablePromise<ByteBuf> read = new SettablePromise<>();
		this.read = read;
		if (scheduledReadTimeout == null && readTimeout != NO_TIMEOUT) {
			scheduleReadTimeout();
		}
		if (ops >= 0) {
			updateInterests();
		}
		return read;
	}

	@Override
	public void onReadReady() {
		ops = (byte) (ops | 0x80);
		try {
			doRead();
		} catch (IOException e) {
			closeEx(e);
			return;
		}
		if (read != null && (readBuf != null || readEndOfStream)) {
			SettablePromise<@Nullable ByteBuf> read = this.read;
			ByteBuf readBuf = this.readBuf;
			this.read = null;
			this.readBuf = null;
			read.set(readBuf);
		}
		if (isClosed()) return;
		ops = (byte) (ops & 0x7f);
		updateInterests();
	}

	private void doRead() throws IOException {
		assert channel != null;
		ByteBuf buf;
		if (DEBUG_READ_OFFSET == 0) {
			buf = ByteBufPool.allocate(readBufferSize);
		} else {
			checkState(DEBUG_READ_OFFSET > 0);

			buf = ByteBufPool.allocate(readBufferSize);
			buf.tail(DEBUG_READ_OFFSET);
			buf.head(DEBUG_READ_OFFSET);
		}
		ByteBuffer buffer = buf.toWriteByteBuffer();

		int numRead;
		try {
			numRead = channel.read(buffer);
			buf.ofWriteByteBuffer(buffer);
		} catch (IOException e) {
			buf.recycle();
			if (inspector != null) inspector.onReadError(this, e);
			throw e;
		}

		if (numRead == 0) {
			if (inspector != null) inspector.onRead(this, buf);
			buf.recycle();
			return;
		}

		scheduledReadTimeout = nullify(scheduledReadTimeout, ScheduledRunnable::cancel);

		if (numRead == -1) {
			buf.recycle();
			if (inspector != null) inspector.onReadEndOfStream(this);
			readEndOfStream = true;
			if (writeEndOfStream && writeBuf == null) {
				doClose();
			}
			return;
		}

		if (inspector != null) inspector.onRead(this, buf);

		if (readBuf == null) {
			readBuf = buf;
		} else {
			readBuf = ByteBufPool.ensureWriteRemaining(readBuf, buf.readRemaining());
			readBuf.put(buf.array(), buf.head(), buf.readRemaining());
			buf.recycle();
		}
	}

	// write cycle
	@Override
	public Promise<Void> write(@Nullable ByteBuf buf) {
		if (CHECK) {
			checkState(inReactorThread());
			checkState(!writeEndOfStream, "End of stream has already been sent");
		}
		if (isClosed()) {
			if (buf != null) buf.recycle();
			return Promise.ofException(new AsyncCloseException());
		}
		writeEndOfStream |= buf == null;

		if (writeBuf == null) {
			if (buf != null && !buf.canRead()) {
				buf.recycle();
				return Promise.complete();
			}
			writeBuf = buf;
		} else {
			if (buf != null) {
				writeBuf = ByteBufPool.ensureWriteRemaining(this.writeBuf, buf.readRemaining());
				writeBuf.put(buf.array(), buf.head(), buf.readRemaining());
				buf.recycle();
			}
		}

		if (write != null) return write;

		try {
			doWrite();
		} catch (IOException e) {
			closeEx(e);
			return Promise.ofException(e);
		}

		if (this.writeBuf == null) {
			return Promise.complete();
		}
		SettablePromise<Void> write = new SettablePromise<>();
		this.write = write;
		if (scheduledWriteTimeout == null && writeTimeout != NO_TIMEOUT) {
			scheduleWriteTimeout();
		}
		if (ops >= 0) {
			updateInterests();
		}
		return write;
	}

	@Override
	public boolean isReadAvailable() {
		return readBuf != null;
	}

	@Override
	public void onWriteReady() {
		assert write != null;
		ops = (byte) (ops | 0x80);
		try {
			doWrite();
		} catch (IOException e) {
			closeEx(e);
			return;
		}
		if (writeBuf == null) {
			SettablePromise<@Nullable Void> write = this.write;
			this.write = null;
			write.set(null);
		}
		if (isClosed()) return;
		ops = (byte) (ops & 0x7f);
		updateInterests();
	}

	private void doWrite() throws IOException {
		assert channel != null;
		if (writeBuf != null) {
			ByteBuf buf = this.writeBuf;
			ByteBuffer buffer = buf.toReadByteBuffer();

			try {
				channel.write(buffer);
			} catch (IOException e) {
				if (inspector != null) inspector.onWriteError(this, e);
				throw e;
			}

			if (inspector != null) inspector.onWrite(this, buf, buffer.position() - buf.head());

			buf.ofReadByteBuffer(buffer);

			if (buf.canRead()) {
				return;
			} else {
				buf.recycle();
				writeBuf = null;
			}
		}

		scheduledWriteTimeout = nullify(scheduledWriteTimeout, ScheduledRunnable::cancel);

		if (writeEndOfStream) {
			if (readEndOfStream) {
				doClose();
			} else {
				channel.shutdownOutput();
			}
		}
	}

	@Override
	public void closeEx(Exception e) {
		if (CHECK) checkState(inReactorThread());
		if (isClosed()) return;
		doClose();
		readBuf = nullify(readBuf, ByteBuf::recycle);
		writeBuf = nullify(writeBuf, ByteBuf::recycle);
		scheduledReadTimeout = nullify(scheduledReadTimeout, ScheduledRunnable::cancel);
		scheduledWriteTimeout = nullify(scheduledWriteTimeout, ScheduledRunnable::cancel);
		read = nullify(read, SettablePromise::setException, e);
		write = nullify(write, SettablePromise::setException, e);
	}

	private void doClose() {
		reactor.closeChannel(channel, key);
		channel = null;
		CONNECTION_COUNT.decrementAndGet();
		if (inspector != null) inspector.onDisconnect(this);
	}

	@Override
	public boolean isClosed() {
		return channel == null;
	}

	public @Nullable SocketChannel getSocketChannel() {
		return channel;
	}

	@Override
	public String toString() {
		return "AsyncTcpSocketImpl{" +
				"channel=" + (channel != null ? channel : "") +
				", readBuf=" + readBuf +
				", writeBuf=" + writeBuf +
				", readEndOfStream=" + readEndOfStream +
				", writeEndOfStream=" + writeEndOfStream +
				", read=" + read +
				", write=" + write +
				", ops=" + ops +
				"}";
	}
}
