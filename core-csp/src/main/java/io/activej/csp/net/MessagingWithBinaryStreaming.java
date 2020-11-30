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

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.parse.TruncatedDataException;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.ChannelSuppliers;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;

/**
 * Represents a simple binary protocol over for communication a TCP connection.
 */
public final class MessagingWithBinaryStreaming<I, O> implements Messaging<I, O> {
	private static final TruncatedDataException UNEXPECTED_END_OF_STREAM = new TruncatedDataException(MessagingWithBinaryStreaming.class);

	private final AsyncTcpSocket socket;

	private final ByteBufsCodec<I, O> codec;

	private final ByteBufQueue bufs = new ByteBufQueue();
	private final BinaryChannelSupplier bufsSupplier;

	private Throwable closedException;

	private boolean readDone;
	private boolean writeDone;

	// region creators
	private MessagingWithBinaryStreaming(AsyncTcpSocket socket, ByteBufsCodec<I, O> codec) {
		this.socket = socket;
		this.codec = codec;
		this.bufsSupplier = BinaryChannelSupplier.ofProvidedQueue(bufs,
				() -> this.socket.read()
						.then(buf -> {
							if (buf != null) {
								bufs.add(buf);
								return Promise.complete();
							} else {
								return Promise.ofException(UNEXPECTED_END_OF_STREAM);
							}
						})
						.whenException(this::closeEx),
				Promise::complete,
				this);
	}

	public static <I, O> MessagingWithBinaryStreaming<I, O> create(AsyncTcpSocket socket,
			ByteBufsCodec<I, O> serializer) {
		MessagingWithBinaryStreaming<I, O> messaging = new MessagingWithBinaryStreaming<>(socket, serializer);
		messaging.prefetch();
		return messaging;
	}
	// endregion

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
		return bufsSupplier.parse(codec::tryDecode)
				.whenResult(this::prefetch)
				.whenException(this::closeEx);
	}

	@Override
	public Promise<Void> send(O msg) {
		return socket.write(codec.encode(msg));
	}

	@Override
	public Promise<Void> sendEndOfStream() {
		return socket.write(null)
				.whenResult(() -> {
					writeDone = true;
					closeIfDone();
				})
				.whenException(this::closeEx);
	}

	@Override
	public ChannelConsumer<ByteBuf> sendBinaryStream() {
		return ChannelConsumer.ofSocket(socket)
				.withAcknowledgement(ack -> ack
						.whenResult(() -> {
							writeDone = true;
							closeIfDone();
						}));
	}

	@Override
	public ChannelSupplier<ByteBuf> receiveBinaryStream() {
		return ChannelSuppliers.concat(ChannelSupplier.ofIterator(bufs.asIterator()), ChannelSupplier.ofSocket(socket))
				.withEndOfStream(eos -> eos
						.whenResult(() -> {
							readDone = true;
							closeIfDone();
						}));
	}

	@Override
	public void closeEx(@NotNull Throwable e) {
		if (isClosed()) return;
		closedException = e;
		socket.closeEx(e);
		bufs.recycle();
	}

	private void closeIfDone() {
		if (readDone && writeDone) {
			close();
		}
	}

	public boolean isClosed() {
		return closedException != null;
	}

	@Override
	public String toString() {
		return "MessagingWithBinaryStreaming{socket=" + socket + "}";
	}
}
