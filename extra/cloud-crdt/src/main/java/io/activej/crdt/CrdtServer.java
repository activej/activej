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
import io.activej.csp.net.Messaging;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.stats.StreamStats;
import io.activej.datastream.stats.StreamStatsBasic;
import io.activej.datastream.stats.StreamStatsDetailed;
import io.activej.eventloop.Eventloop;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.net.AbstractServer;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import io.activej.serializer.BinarySerializer;

import java.net.InetAddress;
import java.time.Duration;
import java.util.function.Function;

import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.thisMethod;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.crdt.CrdtMessagingProto.CrdtRequest.RequestCase.TAKE_ACK;
import static io.activej.crdt.CrdtMessagingProto.CrdtResponse.ResponseCase.*;
import static io.activej.crdt.util.Utils.ackTransformer;
import static io.activej.crdt.util.Utils.codec;

@SuppressWarnings("rawtypes")
public final class CrdtServer<K extends Comparable<K>, S> extends AbstractServer<CrdtServer<K, S>> {
	public static final Version VERSION = Version.newBuilder().setMajor(1).setMinor(0).build();

	private static final ByteBufsCodec<CrdtRequest, CrdtResponse> SERIALIZER = codec(CrdtRequest.parser());

	private Function<CrdtRequest.Handshake, CrdtResponse.Handshake> handshakeHandler = $ -> Handshake.newBuilder()
			.setOk(Ok.newBuilder())
			.build();

	private final CrdtStorage<K, S> storage;
	private final CrdtDataSerializer<K, S> serializer;
	private final BinarySerializer<CrdtTombstone<K>> tombstoneSerializer;

	// region JMX
	private boolean detailedStats;

	private final StreamStatsBasic<CrdtData<K, S>> uploadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> uploadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<CrdtData<K, S>> downloadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> downloadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<CrdtData<K, S>> takeStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> takeStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<CrdtTombstone<K>> removeStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtTombstone<K>> removeStatsDetailed = StreamStats.detailed();

	private final PromiseStats handshakePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadFinishedPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadFinishedPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats removeBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats removeFinishedPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats takeBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats takeFinishedPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats pingPromise = PromiseStats.create(Duration.ofMinutes(5));
	// endregion

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
					return handshake(messaging, handshakeMsg.getHandshake())
							.then(messaging::receive)
							.then(msg -> switch (msg.getRequestCase()) {
								case DOWNLOAD -> download(messaging, msg.getDownload());
								case UPLOAD -> upload(messaging, msg.getUpload());
								case REMOVE -> remove(messaging, msg.getRemove());
								case PING -> ping(messaging, msg.getPing());
								case TAKE -> take(messaging, msg.getTake());
								case HANDSHAKE ->
										Promise.ofException(new CrdtException("Handshake was already performed"));
								case REQUEST_NOT_SET ->
										Promise.ofException(new CrdtException("Request was not set"));
								default ->
										Promise.ofException(new CrdtException("Unknown message type: " + msg.getRequestCase()));
							});
				})
				.whenException(e -> {
					logger.warn("got an error while handling message {}", this, e);
					messaging.send(errorResponse(e))
							.then(messaging::sendEndOfStream)
							.whenResult(messaging::close);
				});
	}

	private Promise<Void> handshake(Messaging<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Handshake handshake) {
		CrdtResponse handShakeResponse = CrdtResponse.newBuilder()
				.setHandshake(handshakeHandler.apply(handshake))
				.build();

		return messaging.send(handShakeResponse)
				.whenComplete(handshakePromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, thisMethod(), messaging, handshake, this));
	}

	private Promise<Void> take(MessagingWithBinaryStreaming<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Take take) {
		return storage.take()
				.whenComplete(takeBeginPromise.recordStats())
				.whenResult(() -> messaging.send(response(TAKE_STARTED)))
				.then(supplier -> supplier
						.transformWith(ackTransformer(ack -> ack
								.then(() -> messaging.receive()
										.then(takeAck -> {
											if (!takeAck.hasTakeAck()) {
												return Promise.ofException(new CrdtException("Received message " + takeAck + " instead of " + TAKE_ACK));
											}
											return Promise.complete();
										}))))
						.transformWith(detailedStats ? takeStatsDetailed : takeStats)
						.transformWith(ChannelSerializer.create(serializer))
						.streamTo(messaging.sendBinaryStream()))
				.whenComplete(takeFinishedPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, thisMethod(), messaging, take, this));
	}

	private Promise<Void> ping(MessagingWithBinaryStreaming<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Ping ping) {
		return messaging.send(response(PONG))
				.then(messaging::sendEndOfStream)
				.whenResult(messaging::close)
				.whenComplete(pingPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, thisMethod(), messaging, ping, this));
	}

	private Promise<Void> remove(MessagingWithBinaryStreaming<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Remove remove) {
		return messaging.receiveBinaryStream()
				.transformWith(ChannelDeserializer.create(tombstoneSerializer))
				.streamTo(StreamConsumer.ofPromise(storage.remove()
						.map(consumer -> consumer.transformWith(detailedStats ? removeStatsDetailed : removeStats))
						.whenComplete(removeBeginPromise.recordStats())))
				.then(() -> messaging.send(response(REMOVE_ACK)))
				.then(messaging::sendEndOfStream)
				.whenResult(messaging::close)
				.whenComplete(removeFinishedPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, thisMethod(), messaging, remove, this));
	}

	private Promise<Void> upload(MessagingWithBinaryStreaming<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Upload upload) {
		return messaging.receiveBinaryStream()
				.transformWith(ChannelDeserializer.create(serializer))
				.streamTo(StreamConsumer.ofPromise(storage.upload()
						.map(consumer -> consumer.transformWith(detailedStats ? uploadStatsDetailed : uploadStats))
						.whenComplete(uploadBeginPromise.recordStats())))
				.then(() -> messaging.send(response(UPLOAD_ACK)))
				.then(messaging::sendEndOfStream)
				.whenResult(messaging::close)
				.whenComplete(uploadFinishedPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, thisMethod(), messaging, upload, this));
	}

	private Promise<Void> download(MessagingWithBinaryStreaming<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Download download) {
		return storage.download(download.getToken())
				.map(consumer -> consumer.transformWith(detailedStats ? downloadStatsDetailed : downloadStats))
				.whenComplete(downloadBeginPromise.recordStats())
				.whenResult(() -> messaging.send(response(DOWNLOAD_STARTED)))
				.then(supplier -> supplier
						.transformWith(ChannelSerializer.create(serializer))
						.streamTo(messaging.sendBinaryStream()))
				.whenComplete(downloadFinishedPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, thisMethod(), messaging, download, this));
	}

	private static CrdtResponse response(ResponseCase responseCase) {
		CrdtResponse.Builder builder = CrdtResponse.newBuilder();
		return switch (responseCase) {
			case UPLOAD_ACK -> builder.setUploadAck(UploadAck.newBuilder()).build();
			case REMOVE_ACK -> builder.setRemoveAck(RemoveAck.newBuilder()).build();
			case PONG -> builder.setPong(Pong.newBuilder()).build();
			case DOWNLOAD_STARTED -> builder.setDownloadStarted(DownloadStarted.newBuilder()).build();
			case TAKE_STARTED -> builder.setTakeStarted(TakeStarted.newBuilder()).build();
			default -> throw new AssertionError();
		};
	}

	private static CrdtResponse errorResponse(Exception exception) {
		return CrdtResponse.newBuilder()
				.setServerError(ServerError.newBuilder()
						.setMessage(exception.getClass().getSimpleName() + ": " + exception.getMessage()))
				.build();
	}

	// region JMX
	@JmxAttribute
	public boolean isDetailedStats() {
		return detailedStats;
	}

	@JmxOperation
	public void startDetailedMonitoring() {
		detailedStats = true;
	}

	@JmxOperation
	public void stopDetailedMonitoring() {
		detailedStats = false;
	}

	@JmxAttribute
	public StreamStatsBasic getUploadStats() {
		return uploadStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getUploadStatsDetailed() {
		return uploadStatsDetailed;
	}

	@JmxAttribute
	public StreamStatsBasic getDownloadStats() {
		return downloadStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getDownloadStatsDetailed() {
		return downloadStatsDetailed;
	}

	@JmxAttribute
	public StreamStatsBasic getTakeStats() {
		return takeStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getTakeStatsDetailed() {
		return takeStatsDetailed;
	}

	@JmxAttribute
	public StreamStatsBasic getRemoveStats() {
		return removeStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getRemoveStatsDetailed() {
		return removeStatsDetailed;
	}

	@JmxAttribute
	public PromiseStats getHandshakePromise() {
		return handshakePromise;
	}

	@JmxAttribute
	public PromiseStats getDownloadBeginPromise() {
		return downloadBeginPromise;
	}

	@JmxAttribute
	public PromiseStats getDownloadFinishedPromise() {
		return downloadFinishedPromise;
	}

	@JmxAttribute
	public PromiseStats getUploadBeginPromise() {
		return uploadBeginPromise;
	}

	@JmxAttribute
	public PromiseStats getUploadFinishedPromise() {
		return uploadFinishedPromise;
	}

	@JmxAttribute
	public PromiseStats getRemoveBeginPromise() {
		return removeBeginPromise;
	}

	@JmxAttribute
	public PromiseStats getRemoveFinishedPromise() {
		return removeFinishedPromise;
	}

	@JmxAttribute
	public PromiseStats getTakeBeginPromise() {
		return takeBeginPromise;
	}

	@JmxAttribute
	public PromiseStats getTakeFinishedPromise() {
		return takeFinishedPromise;
	}

	@JmxAttribute
	public PromiseStats getPingPromise() {
		return pingPromise;
	}
	// endregion
}
