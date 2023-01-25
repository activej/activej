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
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.crdt.util.BinarySerializer_CrdtData;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.net.IMessaging;
import io.activej.csp.net.Messaging;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.stats.StreamStats;
import io.activej.datastream.stats.StreamStats_Basic;
import io.activej.datastream.stats.StreamStats_Detailed;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.net.AbstractReactiveServer;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import io.activej.reactor.nio.NioReactor;
import io.activej.serializer.BinarySerializer;

import java.net.InetAddress;
import java.time.Duration;
import java.util.function.Function;

import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.thisMethod;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.crdt.util.Utils.*;

@SuppressWarnings("rawtypes")
public final class CrdtServer<K extends Comparable<K>, S> extends AbstractReactiveServer {
	public static final Version VERSION = new Version(1, 0);

	private static final ByteBufsCodec<CrdtRequest, CrdtResponse> SERIALIZER = ByteBufsCodec.ofStreamCodecs(
			CRDT_REQUEST_CODEC,
			CRDT_RESPONSE_CODEC
	);

	private Function<CrdtRequest.Handshake, CrdtResponse.Handshake> handshakeHandler = $ ->
			new CrdtResponse.Handshake(null);

	private final ICrdtStorage<K, S> storage;
	private final BinarySerializer_CrdtData<K, S> serializer;
	private final BinarySerializer<CrdtTombstone<K>> tombstoneSerializer;

	// region JMX
	private boolean detailedStats;

	private final StreamStats_Basic<CrdtData<K, S>> uploadStats = StreamStats.basic();
	private final StreamStats_Detailed<CrdtData<K, S>> uploadStatsDetailed = StreamStats.detailed();
	private final StreamStats_Basic<CrdtData<K, S>> downloadStats = StreamStats.basic();
	private final StreamStats_Detailed<CrdtData<K, S>> downloadStatsDetailed = StreamStats.detailed();
	private final StreamStats_Basic<CrdtData<K, S>> takeStats = StreamStats.basic();
	private final StreamStats_Detailed<CrdtData<K, S>> takeStatsDetailed = StreamStats.detailed();
	private final StreamStats_Basic<CrdtTombstone<K>> removeStats = StreamStats.basic();
	private final StreamStats_Detailed<CrdtTombstone<K>> removeStatsDetailed = StreamStats.detailed();

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

	private CrdtServer(NioReactor reactor, ICrdtStorage<K, S> storage, BinarySerializer_CrdtData<K, S> serializer) {
		super(reactor);
		this.storage = storage;
		this.serializer = serializer;

		tombstoneSerializer = serializer.getTombstoneSerializer();
	}

	public static <K extends Comparable<K>, S> CrdtServer<K, S>.Builder builder(
			NioReactor reactor,
			ICrdtStorage<K, S> storage,
			BinarySerializer_CrdtData<K, S> serializer
	) {
		return new CrdtServer<>(reactor, storage, serializer).new Builder();
	}

	public static <K extends Comparable<K>, S> CrdtServer<K, S>.Builder builder(
			NioReactor reactor,
			ICrdtStorage<K, S> storage,
			BinarySerializer<K> keySerializer,
			BinarySerializer<S> stateSerializer
	) {
		return new CrdtServer<>(reactor, storage, new BinarySerializer_CrdtData<>(keySerializer, stateSerializer)).new Builder();
	}

	public final class Builder extends AbstractReactiveServer.Builder<Builder, CrdtServer<K, S>> {
		private Builder() {}

		public Builder withHandshakeHandler(Function<CrdtRequest.Handshake, CrdtResponse.Handshake> handshakeHandler) {
			checkNotBuilt(this);
			CrdtServer.this.handshakeHandler = handshakeHandler;
			return this;
		}
	}

	@Override
	protected void serve(AsyncTcpSocket socket, InetAddress remoteAddress) {
		Messaging<CrdtRequest, CrdtResponse> messaging =
				Messaging.create(socket, SERIALIZER);
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

	private Promise<Void> dispatch(Messaging<CrdtRequest, CrdtResponse> messaging, CrdtRequest msg) {
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

	private Promise<Void> handleHandshake(IMessaging<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Handshake handshake) {
		return messaging.send(handshakeHandler.apply(handshake))
				.whenComplete(handshakePromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, thisMethod(), messaging, handshake, this));
	}

	private Promise<Void> handleTake(Messaging<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Take take) {
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

	private Promise<Void> handlePing(Messaging<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Ping ping) {
		return messaging.send(new CrdtResponse.Pong())
				.then(messaging::sendEndOfStream)
				.whenResult(messaging::close)
				.whenComplete(pingPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, thisMethod(), messaging, ping, this));
	}

	private Promise<Void> handleRemove(Messaging<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Remove remove) {
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

	private Promise<Void> handleUpload(Messaging<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Upload upload) {
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

	private Promise<Void> handleDownload(Messaging<CrdtRequest, CrdtResponse> messaging, CrdtRequest.Download download) {
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
	public StreamStats_Basic getUploadStats() {
		return uploadStats;
	}

	@JmxAttribute
	public StreamStats_Detailed getUploadStatsDetailed() {
		return uploadStatsDetailed;
	}

	@JmxAttribute
	public StreamStats_Basic getDownloadStats() {
		return downloadStats;
	}

	@JmxAttribute
	public StreamStats_Detailed getDownloadStatsDetailed() {
		return downloadStatsDetailed;
	}

	@JmxAttribute
	public StreamStats_Basic getTakeStats() {
		return takeStats;
	}

	@JmxAttribute
	public StreamStats_Detailed getTakeStatsDetailed() {
		return takeStatsDetailed;
	}

	@JmxAttribute
	public StreamStats_Basic getRemoveStats() {
		return removeStats;
	}

	@JmxAttribute
	public StreamStats_Detailed getRemoveStatsDetailed() {
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
