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

import io.activej.crdt.CrdtMessagingProto.CrdtRequest;
import io.activej.crdt.CrdtMessagingProto.CrdtResponse;
import io.activej.crdt.CrdtMessagingProto.CrdtResponse.*;
import io.activej.crdt.CrdtMessagingProto.CrdtResponse.Handshake.Ok;
import io.activej.crdt.CrdtMessagingProto.Version;
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
import java.util.function.Function;

import static io.activej.crdt.CrdtMessagingProto.CrdtRequest.RequestCase.TAKE_ACK;
import static io.activej.crdt.CrdtMessagingProto.CrdtResponse.ResponseCase.*;
import static io.activej.fs.util.ProtobufUtils.codec;

public final class CrdtServer<K extends Comparable<K>, S> extends AbstractServer<CrdtServer<K, S>> {
	public static final Version VERSION = Version.newBuilder().setMajor(1).setMinor(0).build();

	private static final ByteBufsCodec<CrdtRequest, CrdtResponse> SERIALIZER = codec(CrdtRequest.parser());

	private Function<CrdtRequest.Handshake, CrdtResponse.Handshake> handshakeHandler = $ -> Handshake.newBuilder()
			.setOk(Ok.newBuilder())
			.build();

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

	public CrdtServer<K, S> withHandshakeHandler(Function<CrdtRequest.Handshake, CrdtResponse.Handshake> handshakeHandler) {
		this.handshakeHandler = handshakeHandler;
		return this;
	}

	@Override
	protected void serve(AsyncTcpSocket socket, InetAddress remoteAddress) {
		MessagingWithBinaryStreaming<CrdtRequest, CrdtResponse> messaging =
				MessagingWithBinaryStreaming.create(socket, SERIALIZER);
		messaging.receive()
				.then(handshakeMsg -> {
					if (!handshakeMsg.hasHandshake()) {
						return Promise.ofException(new CrdtException("Handshake expected"));
					}
					CrdtResponse handshakeResponse = CrdtResponse.newBuilder()
							.setHandshake(handshakeHandler.apply(handshakeMsg.getHandshake()))
							.build();
					return messaging.send(handshakeResponse)
							.then(messaging::receive)
							.then(msg -> {
								switch (msg.getRequestCase()) {
									case DOWNLOAD:
										return storage.download((msg.getDownload()).getToken())
												.whenResult(() -> messaging.send(response(DOWNLOAD_STARTED)))
												.then(supplier -> supplier
														.transformWith(ChannelSerializer.create(serializer))
														.streamTo(messaging.sendBinaryStream()));
									case UPLOAD:
										return messaging.receiveBinaryStream()
												.transformWith(ChannelDeserializer.create(serializer))
												.streamTo(StreamConsumer.ofPromise(storage.upload()))
												.then(() -> messaging.send(response(UPLOAD_ACK)))
												.then(messaging::sendEndOfStream)
												.whenResult(messaging::close);
									case REMOVE:
										return messaging.receiveBinaryStream()
												.transformWith(ChannelDeserializer.create(tombstoneSerializer))
												.streamTo(StreamConsumer.ofPromise(storage.remove()))
												.then(() -> messaging.send(response(REMOVE_ACK)))
												.then(messaging::sendEndOfStream)
												.whenResult(messaging::close);
									case PING:
										return messaging.send(response(PONG))
												.then(messaging::sendEndOfStream)
												.whenResult(messaging::close);
									case TAKE:
										return storage.take()
												.whenResult(() -> messaging.send(response(TAKE_STARTED)))
												.then(supplier -> supplier
														.transformWith(ChannelSerializer.create(serializer))
														.streamTo(messaging.sendBinaryStream()
																.withAcknowledgement(ack -> ack
																		.then(() -> messaging.receive()
																				.then(takeAck -> {
																					if (!takeAck.hasTakeAck()) {
																						return Promise.ofException(new CrdtException("Received message " + takeAck + " instead of " + TAKE_ACK));
																					}
																					return Promise.complete();
																				})))));
									case HANDSHAKE:
										return Promise.ofException(new CrdtException("Handshake was already performed"));
									case REQUEST_NOT_SET:
										return Promise.ofException(new CrdtException("Request was not set"));
									default:
										return Promise.ofException(new CrdtException("Unknown message type: " + msg.getRequestCase()));
								}
							});
				})
				.whenException(e -> {
					logger.warn("got an error while handling message {}", this, e);
					messaging.send(errorResponse(e))
							.then(messaging::sendEndOfStream)
							.whenResult(messaging::close);
				});
	}

	private static CrdtResponse response(ResponseCase responseCase) {
		CrdtResponse.Builder builder = CrdtResponse.newBuilder();
		switch (responseCase) {
			case UPLOAD_ACK:
				return builder.setUploadAck(UploadAck.newBuilder()).build();
			case REMOVE_ACK:
				return builder.setRemoveAck(RemoveAck.newBuilder()).build();
			case PONG:
				return builder.setPong(Pong.newBuilder()).build();
			case DOWNLOAD_STARTED:
				return builder.setDownloadStarted(DownloadStarted.newBuilder()).build();
			case TAKE_STARTED:
				return builder.setTakeStarted(TakeStarted.newBuilder()).build();
			default:
				throw new AssertionError();
		}
	}

	private static CrdtResponse errorResponse(Exception exception) {
		return CrdtResponse.newBuilder()
				.setServerError(ServerError.newBuilder()
						.setMessage(exception.getClass().getSimpleName() + ": " + exception.getMessage()))
				.build();
	}
}
