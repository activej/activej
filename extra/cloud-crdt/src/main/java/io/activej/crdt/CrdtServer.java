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

import io.activej.crdt.messaging.CrdtRequest;
import io.activej.crdt.messaging.CrdtResponse;
import io.activej.crdt.messaging.Version;
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
import io.activej.fs.util.RemoteFsUtils;
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
import static io.activej.crdt.util.Utils.*;

@SuppressWarnings("rawtypes")
public final class CrdtServer<K extends Comparable<K>, S> extends AbstractServer<CrdtServer<K, S>> {
	public static final Version VERSION = new Version(1, 0);

	private static final ByteBufsCodec<CrdtRequest, CrdtResponse> SERIALIZER = RemoteFsUtils.codec(
			CRDT_REQUEST_CODEC,
			CRDT_RESPONSE_CODEC
	);

	private Function<CrdtRequest.Handshake, CrdtResponse.Handshake> handshakeHandler = $ ->
			new CrdtResponse.Handshake(null);

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
				.then(request -> {
					if (!(request instanceof CrdtRequest.Handshake handshake)) {
						return Promise.ofException(new CrdtException("Handshake expected"));
					}
					return handleHandshake(messaging, handshake);
				})
				.then(messaging::receive)
				.then(msg -> dispatch(messaging, msg))
				.whenException(e -> {
					logger.warn("got an error while handling message {}", this, e);
					messaging.send(new CrdtResponse.ServerError(e.getClass().getSimpleName() + ": " + e.getMessage()))
							.then(messaging::sendEndOfStream)
							.whenResult(messaging::close);
				});
	}

	private Promise<Void> dispatch(MessagingWithBinaryStreaming<CrdtRequest, CrdtResponse> messaging, CrdtRequest msg) {
		if (msg instanceof CrdtRequest.Download download) {
			return handleDownload(messaging, download);
		}
		if (msg instanceof CrdtRequest.Upload upload) {
			return handleUpload(messaging, upload);
		}
		if (msg instanceof CrdtRequest.Remove remove) {
			return handleRemove(messaging, remove);
		}
		if (msg instanceof CrdtRequest.Ping ping) {
			return handlePing(messaging, ping);
		}
		if (msg instanceof CrdtRequest.Take take) {
			return handleTake(messaging, take);
		}
		if (msg instanceof CrdtRequest.Handshake) {
			return Promise.ofException(new CrdtException("Handshake was already performed"));
		}
		throw new AssertionError();
	}

	private Promise<Void> handleHandshake(Messaging<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Handshake handshake) {
		return messaging.send(handshakeHandler.apply(handshake))
				.whenComplete(handshakePromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, thisMethod(), messaging, handshake, this));
	}

	private Promise<Void> handleTake(MessagingWithBinaryStreaming<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Take take) {
		return storage.take()
				.whenComplete(takeBeginPromise.recordStats())
				.whenResult(() -> messaging.send(new CrdtResponse.TakeStarted()))
				.then(supplier -> supplier
						.transformWith(ackTransformer(ack -> ack
								.then(messaging::receive)
								.then(msg -> {
									if (!(msg instanceof CrdtRequest.TakeAck)) {
										return Promise.ofException(new CrdtException(
												"Received message " + msg +
														" instead of " + CrdtRequest.TakeAck.class
										));
									}
									return Promise.complete();
								})))
						.transformWith(detailedStats ? takeStatsDetailed : takeStats)
						.transformWith(ChannelSerializer.create(serializer))
						.streamTo(messaging.sendBinaryStream()))
				.whenComplete(takeFinishedPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, thisMethod(), messaging, take, this));
	}

	private Promise<Void> handlePing(MessagingWithBinaryStreaming<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Ping ping) {
		return messaging.send(new CrdtResponse.Pong())
				.then(messaging::sendEndOfStream)
				.whenResult(messaging::close)
				.whenComplete(pingPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, thisMethod(), messaging, ping, this));
	}

	private Promise<Void> handleRemove(MessagingWithBinaryStreaming<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Remove remove) {
		return messaging.receiveBinaryStream()
				.transformWith(ChannelDeserializer.create(tombstoneSerializer))
				.streamTo(StreamConsumer.ofPromise(storage.remove()
						.map(consumer -> consumer.transformWith(detailedStats ? removeStatsDetailed : removeStats))
						.whenComplete(removeBeginPromise.recordStats())))
				.then(() -> messaging.send(new CrdtResponse.RemoveAck()))
				.then(messaging::sendEndOfStream)
				.whenResult(messaging::close)
				.whenComplete(removeFinishedPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, thisMethod(), messaging, remove, this));
	}

	private Promise<Void> handleUpload(MessagingWithBinaryStreaming<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Upload upload) {
		return messaging.receiveBinaryStream()
				.transformWith(ChannelDeserializer.create(serializer))
				.streamTo(StreamConsumer.ofPromise(storage.upload()
						.map(consumer -> consumer.transformWith(detailedStats ? uploadStatsDetailed : uploadStats))
						.whenComplete(uploadBeginPromise.recordStats())))
				.then(() -> messaging.send(new CrdtResponse.UploadAck()))
				.then(messaging::sendEndOfStream)
				.whenResult(messaging::close)
				.whenComplete(uploadFinishedPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, thisMethod(), messaging, upload, this));
	}

	private Promise<Void> handleDownload(MessagingWithBinaryStreaming<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Download download) {
		return storage.download(download.token())
				.map(consumer -> consumer.transformWith(detailedStats ? downloadStatsDetailed : downloadStats))
				.whenComplete(downloadBeginPromise.recordStats())
				.whenResult(() -> messaging.send(new CrdtResponse.DownloadStarted()))
				.then(supplier -> supplier
						.transformWith(ChannelSerializer.create(serializer))
						.streamTo(messaging.sendBinaryStream()))
				.whenComplete(downloadFinishedPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, thisMethod(), messaging, download, this));
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
