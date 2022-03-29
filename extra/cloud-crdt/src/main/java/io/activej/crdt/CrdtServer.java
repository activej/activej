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

package io.activej.crdt;

import com.google.protobuf.InvalidProtocolBufferException;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.exception.MalformedDataException;
import io.activej.crdt.CrdtMessagingProto.CrdtMessage;
import io.activej.crdt.CrdtMessagingProto.CrdtMessage.CrdtMessages;
import io.activej.crdt.CrdtMessagingProto.CrdtResponse;
import io.activej.crdt.CrdtMessagingProto.CrdtResponse.CrdtResponses;
import io.activej.crdt.CrdtMessagingProto.CrdtResponse.ServerError;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.net.AbstractServer;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.Promise;
import io.activej.serializer.BinarySerializer;

import java.net.InetAddress;

import static io.activej.crdt.CrdtMessagingProto.CrdtMessage.CrdtMessages.TAKE_FINISHED;
import static io.activej.crdt.CrdtMessagingProto.CrdtResponse.CrdtResponses.*;
import static io.activej.csp.binary.Utils.nullTerminated;

public final class CrdtServer<K extends Comparable<K>, S> extends AbstractServer<CrdtServer<K, S>> {
	private static final ByteBufsCodec<CrdtMessage, CrdtResponse> SERIALIZER =
			nullTerminated()
					.andThen(
							value -> {
								try {
									return CrdtMessage.parseFrom(value.asArray());
								} catch (InvalidProtocolBufferException e) {
									throw new MalformedDataException(e);
								}
							},
							crdtResponse -> ByteBuf.wrapForReading(crdtResponse.toByteArray()));

	private final CrdtStorage<K, S> storage;
	private final CrdtDataSerializer<K, S> serializer;
	private final BinarySerializer<CrdtTombstone<K>> tombstoneSerializer;

	private CrdtServer(Eventloop eventloop, CrdtStorage<K, S> storage, CrdtDataSerializer<K, S> serializer) {
		super(eventloop);
		this.storage = storage;
		this.serializer = serializer;

		tombstoneSerializer = serializer.getTombstoneSerializer();
	}

	public static <K extends Comparable<K>, S> CrdtServer<K, S> create(Eventloop eventloop, CrdtStorage<K, S> storage, CrdtDataSerializer<K, S> serializer) {
		return new CrdtServer<>(eventloop, storage, serializer);
	}

	public static <K extends Comparable<K>, S> CrdtServer<K, S> create(Eventloop eventloop, CrdtStorage<K, S> storage, BinarySerializer<K> keySerializer, BinarySerializer<S> stateSerializer) {
		return new CrdtServer<>(eventloop, storage, new CrdtDataSerializer<>(keySerializer, stateSerializer));
	}

	@Override
	protected void serve(AsyncTcpSocket socket, InetAddress remoteAddress) {
		MessagingWithBinaryStreaming<CrdtMessage, CrdtResponse> messaging =
				MessagingWithBinaryStreaming.create(socket, SERIALIZER);
		messaging.receive()
				.then(msg -> {
					if (msg.hasDownload()) {
						return storage.download((msg.getDownload()).getToken())
								.whenResult(() -> messaging.send(response(DOWNLOAD_STARTED)))
								.then(supplier -> supplier
										.transformWith(ChannelSerializer.create(serializer))
										.streamTo(messaging.sendBinaryStream()));
					}
					if (!msg.hasMessages()) {
						throw new IllegalArgumentException("Empty message");
					}
					CrdtMessages messages = msg.getMessages();
					if (messages == CrdtMessages.UPLOAD) {
						return messaging.receiveBinaryStream()
								.transformWith(ChannelDeserializer.create(serializer))
								.streamTo(StreamConsumer.ofPromise(storage.upload()))
								.then(() -> messaging.send(response(UPLOAD_FINISHED)))
								.then(messaging::sendEndOfStream)
								.whenResult(messaging::close);

					}
					if (messages == CrdtMessages.REMOVE) {
						return messaging.receiveBinaryStream()
								.transformWith(ChannelDeserializer.create(tombstoneSerializer))
								.streamTo(StreamConsumer.ofPromise(storage.remove()))
								.then(() -> messaging.send(response(REMOVE_FINISHED)))
								.then(messaging::sendEndOfStream)
								.whenResult(messaging::close);
					}
					if (messages == CrdtMessages.PING) {
						return messaging.send(response(PONG))
								.then(messaging::sendEndOfStream)
								.whenResult(messaging::close);
					}
					if (messages == CrdtMessages.TAKE) {
						return storage.take()
								.whenResult(() -> messaging.send(response(TAKE_STARTED)))
								.then(supplier -> supplier
										.transformWith(ChannelSerializer.create(serializer))
										.streamTo(messaging.sendBinaryStream()
												.withAcknowledgement(ack -> ack
														.then(() -> messaging.receive()
																.then(takeAck -> {
																	if (!takeAck.hasMessages() && takeAck.getMessages() != TAKE_FINISHED) {
																		return Promise.ofException(new CrdtException("Received message " + takeAck + " instead of " + TAKE_FINISHED));
																	}
																	return Promise.complete();
																})))));
					}
					throw new IllegalArgumentException("Message type was added, but no handling code for it");
				})
				.whenException(e -> {
					logger.warn("got an error while handling message {}", this, e);
					messaging.send(errorResponse(e))
							.then(messaging::sendEndOfStream)
							.whenResult(messaging::close);
				});
	}

	private static CrdtResponse response(CrdtResponses responses) {
		return CrdtResponse.newBuilder().setResponses(responses).build();
	}

	private static CrdtResponse errorResponse(Exception exception) {
		return CrdtResponse.newBuilder()
				.setError(ServerError.newBuilder()
						.setMessage(exception.getClass().getSimpleName() + ": " + exception.getMessage()))
				.build();
	}
}
