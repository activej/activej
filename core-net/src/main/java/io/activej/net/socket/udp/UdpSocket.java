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

package io.activej.net.socket.udp;

import io.activej.async.exception.AsyncCloseException;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.MemSize;
import io.activej.common.inspector.AbstractInspector;
import io.activej.common.inspector.BaseInspector;
import io.activej.common.recycle.Recyclers;
import io.activej.common.tuple.Tuple2;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.EventStats;
import io.activej.jmx.stats.ValueStats;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.AbstractNioReactive;
import io.activej.reactor.nio.NioChannelEventHandler;
import io.activej.reactor.nio.NioReactor;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.time.Duration;
import java.util.ArrayDeque;

import static io.activej.reactor.Reactive.checkInReactorThread;

public final class UdpSocket extends AbstractNioReactive implements AsyncUdpSocket, NioChannelEventHandler {
	private static final int OP_POSTPONED = 1 << 7;  // SelectionKey constant
	private static final MemSize DEFAULT_UDP_BUFFER_SIZE = MemSize.kilobytes(16);

	private @Nullable SelectionKey key;

	private int receiveBufferSize = DEFAULT_UDP_BUFFER_SIZE.toInt();

	private final DatagramChannel channel;

	private final ArrayDeque<SettablePromise<UdpPacket>> readQueue = new ArrayDeque<>();
	private final ArrayDeque<UdpPacket> readBuffer = new ArrayDeque<>();

	private final ArrayDeque<Tuple2<UdpPacket, SettablePromise<Void>>> writeQueue = new ArrayDeque<>();

	private int ops = 0;

	// region JMX
	private @Nullable Inspector inspector;

	public interface Inspector extends BaseInspector<Inspector> {
		void onCreate(UdpSocket socket);

		void onReceive(UdpSocket socket, UdpPacket packet);

		void onReceiveError(UdpSocket socket, IOException e);

		void onSend(UdpSocket socket, UdpPacket packet);

		void onSendError(UdpSocket socket, IOException e);

		void onClose(UdpSocket socket);
	}

	public static class JmxInspector extends AbstractInspector<Inspector> implements Inspector {
		private final EventStats creates;
		private final ValueStats receives;
		private final EventStats receiveErrors;
		private final ValueStats sends;
		private final EventStats sendErrors;
		private final EventStats closes;

		public JmxInspector(Duration smoothingWindow) {
			this.creates = EventStats.create(smoothingWindow);
			this.receives = ValueStats.builder(smoothingWindow)
					.withUnit("bytes")
					.withRate()
					.build();
			this.receiveErrors = EventStats.create(smoothingWindow);
			this.sends = ValueStats.builder(smoothingWindow)
					.withUnit("bytes")
					.withRate()
					.build();
			this.sendErrors = EventStats.create(smoothingWindow);
			this.closes = EventStats.create(smoothingWindow);
		}

		@Override
		public void onCreate(UdpSocket socket) {
			creates.recordEvent();
		}

		@Override
		public void onReceive(UdpSocket socket, UdpPacket packet) {
			receives.recordValue(packet.getBuf().readRemaining());
		}

		@Override
		public void onReceiveError(UdpSocket socket, IOException e) {
			receiveErrors.recordEvent();
		}

		@Override
		public void onSend(UdpSocket socket, UdpPacket packet) {
			sends.recordValue(packet.getBuf().readRemaining());
		}

		@Override
		public void onSendError(UdpSocket socket, IOException e) {
			sendErrors.recordEvent();
		}

		@Override
		public void onClose(UdpSocket socket) {
			closes.recordEvent();
		}

		@JmxAttribute
		public EventStats getCreates() {
			return creates;
		}

		@JmxAttribute(description = "Received packet size")
		public ValueStats getReceives() {
			return receives;
		}

		@JmxAttribute
		public EventStats getReceiveErrors() {
			return receiveErrors;
		}

		@JmxAttribute(description = "Sent packet size")
		public ValueStats getSends() {
			return sends;
		}

		@JmxAttribute
		public EventStats getSendErrors() {
			return sendErrors;
		}

		@JmxAttribute
		public EventStats getCloses() {
			return closes;
		}
	}
	// endregion

	private UdpSocket(NioReactor reactor, DatagramChannel channel) throws IOException {
		super(reactor);
		this.channel = channel;
		this.key = channel.register(reactor.ensureSelector(), 0, this);
	}

	public static Promise<UdpSocket> connect(NioReactor reactor, DatagramChannel channel) {
		try {
			return Promise.of(new UdpSocket(reactor, channel));
		} catch (IOException e) {
			return Promise.ofException(e);
		}
	}

	public void setInspector(@Nullable Inspector inspector) {
		this.inspector = inspector;
	}

	public void setReceiveBufferSize(int receiveBufferSize) {
		this.receiveBufferSize = receiveBufferSize;
	}

	public boolean isOpen() {
		return key != null;
	}

	@Override
	public Promise<UdpPacket> receive() {
		checkInReactorThread(this);
		if (!isOpen()) {
			return Promise.ofException(new AsyncCloseException());
		}
		UdpPacket polled = readBuffer.poll();
		if (polled != null) {
			return Promise.of(polled);
		}
		return Promise.ofCallback(cb -> {
			readQueue.add(cb);
			readInterest(true);
		});
	}

	@Override
	public void onReadReady() {
		checkInReactorThread(this);
		while (isOpen()) {
			ByteBuf buf = ByteBufPool.allocate(receiveBufferSize);
			ByteBuffer buffer = buf.toWriteByteBuffer();
			InetSocketAddress sourceAddress = null;
			try {
				sourceAddress = (InetSocketAddress) channel.receive(buffer);
			} catch (IOException e) {
				if (inspector != null) {
					inspector.onReceiveError(this, e);
				}
			}

			if (sourceAddress == null) {
				buf.recycle();
				break;
			}

			buf.ofWriteByteBuffer(buffer);
			UdpPacket packet = UdpPacket.of(buf, sourceAddress);
			if (inspector != null) {
				inspector.onReceive(this, packet);
			}

			// at this point the packet is *received* so we either
			// complete one of the listening callbacks or store it in the buffer

			SettablePromise<UdpPacket> cb = readQueue.poll();
			if (cb != null) {
				cb.set(packet);
				return;
			}
			readBuffer.add(packet);
		}
	}

	@Override
	public Promise<Void> send(UdpPacket packet) {
		checkInReactorThread(this);
		if (!isOpen()) {
			return Promise.ofException(new AsyncCloseException());
		}
		return Promise.ofCallback(cb -> {
			writeQueue.add(new Tuple2<>(packet, cb));
			onWriteReady();
		});
	}

	@Override
	public void onWriteReady() {
		checkInReactorThread(this);
		while (true) {
			Tuple2<UdpPacket, SettablePromise<Void>> entry = writeQueue.peek();
			if (entry == null) {
				break;
			}
			UdpPacket packet = entry.value1();
			ByteBuffer buffer = packet.getBuf().toReadByteBuffer();

			try {
				if (channel.send(buffer, packet.getSocketAddress()) == 0) {
					break;
				}
			} catch (IOException e) {
				if (inspector != null) {
					inspector.onSendError(this, e);
				}
				break;
			}
			// at this point the packet is *sent* so we poll the queue and recycle the packet
			entry.value2().set(null);

			if (inspector != null) {
				inspector.onSend(this, packet);
			}

			writeQueue.poll();
			packet.recycle();
		}
		writeInterest(!writeQueue.isEmpty());
	}

	private void interests(int newOps) {
		if (ops != newOps) {
			ops = newOps;
			if ((ops & OP_POSTPONED) == 0 && key != null) {
				key.interestOps(ops);
			}
		}
	}

	@SuppressWarnings("SameParameterValue")
	private void readInterest(boolean readInterest) {
		interests(readInterest ? (ops | SelectionKey.OP_READ) : (ops & ~SelectionKey.OP_READ));
	}

	private void writeInterest(boolean writeInterest) {
		interests(writeInterest ? (ops | SelectionKey.OP_WRITE) : (ops & ~SelectionKey.OP_WRITE));
	}

	@Override
	public void close() {
		checkInReactorThread(this);
		SelectionKey key = this.key;
		if (key == null) {
			return;
		}
		this.key = null;
		if (inspector != null) inspector.onClose(this);
		reactor.closeChannel(channel, key);
		Recyclers.recycle(writeQueue);
	}

	@Override
	public String toString() {
		if (isOpen()) {
			return "UDP socket: " + getRemoteSocketAddress();
		}
		return "closed UDP socket";
	}

	private InetSocketAddress getRemoteSocketAddress() {
		try {
			return (InetSocketAddress) channel.getRemoteAddress();
		} catch (ClosedChannelException ignored) {
			throw new AssertionError("Channel is closed");
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}
}
