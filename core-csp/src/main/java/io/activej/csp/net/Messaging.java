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

package io.activej.csp.net;

import io.activej.async.process.AbstractAsyncCloseable;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.TruncatedDataException;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.ChannelSuppliers;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.Promise;

import static io.activej.reactor.Reactive.checkInReactorThread;

/**
 * Represents a simple binary protocol over for communication a TCP connection.
 */
public final class Messaging<I, O> extends AbstractAsyncCloseable implements IMessaging<I, O> {
	private final AsyncTcpSocket socket;

	private final ByteBufsCodec<I, O> codec;

	private final ByteBufs bufs = new ByteBufs();
	private final BinaryChannelSupplier bufsSupplier;

	private boolean readDone;
	private boolean writeDone;

	private Messaging(AsyncTcpSocket socket, ByteBufsCodec<I, O> codec) {
		this.socket = socket;
		this.codec = codec;
		this.bufsSupplier = BinaryChannelSupplier.ofProvidedBufs(bufs,
				() -> this.socket.read()
						.whenResult(buf -> {
							if (buf != null) {
								bufs.add(buf);
							} else {
								throw new TruncatedDataException();
							}
						})
						.toVoid()
						.whenException(this::closeEx),
				Promise::complete,
				this);
	}

	public static <I, O> Messaging<I, O> create(AsyncTcpSocket socket,
			ByteBufsCodec<I, O> serializer) {
		Messaging<I, O> messaging = new Messaging<>(socket, serializer);
		messaging.prefetch();
		return messaging;
	}

	private void prefetch() {
		if (bufs.isEmpty()) {
			socket.read()
					.whenResult(buf -> {
						if (buf != null) {
							bufs.add(buf);
						} else {
							readDone = true;
							closeIfDone();
						}
					})
					.whenException(this::closeEx);
		}
	}

	@Override
	public Promise<I> receive() {
		checkInReactorThread(this);
		return bufsSupplier.decode(codec::tryDecode)
				.whenResult(this::prefetch)
				.whenException(this::closeEx);
	}

	@Override
	public Promise<Void> send(O msg) {
		checkInReactorThread(this);
		return socket.write(codec.encode(msg));
	}

	@Override
	public Promise<Void> sendEndOfStream() {
		checkInReactorThread(this);
		return socket.write(null)
				.whenResult(() -> {
					writeDone = true;
					closeIfDone();
				})
				.whenException(this::closeEx);
	}

	@Override
	public ChannelConsumer<ByteBuf> sendBinaryStream() {
		checkInReactorThread(this);
		return ChannelConsumer.ofSocket(socket)
				.withAcknowledgement(ack -> ack
						.whenResult(() -> {
							writeDone = true;
							closeIfDone();
						}));
	}

	@Override
	public ChannelSupplier<ByteBuf> receiveBinaryStream() {
		checkInReactorThread(this);
		return ChannelSuppliers.concat(ChannelSupplier.ofIterator(bufs.asIterator()), ChannelSupplier.ofSocket(socket))
				.withEndOfStream(eos -> eos
						.whenResult(() -> {
							readDone = true;
							closeIfDone();
						}));
	}

	@Override
	protected void onClosed(Exception e) {
		socket.closeEx(e);
		bufs.recycle();
	}

	private void closeIfDone() {
		if (readDone && writeDone) {
			close();
		}
	}

	@Override
	public String toString() {
		return "MessagingWithBinaryStreaming{socket=" + socket + "}";
	}
}
