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

import io.activej.async.service.ReactiveService;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.ApplicationSettings;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.function.ConsumerEx;
import io.activej.common.function.FunctionEx;
import io.activej.crdt.messaging.CrdtRequest;
import io.activej.crdt.messaging.CrdtResponse;
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.crdt.util.CrdtDataBinarySerializer;
import io.activej.crdt.util.Utils;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.net.Messaging;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.stats.BasicStreamStats;
import io.activej.datastream.stats.DetailedStreamStats;
import io.activej.datastream.stats.StreamStats;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.stats.EventStats;
import io.activej.net.socket.tcp.TcpSocket;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractNioReactive;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;
import io.activej.reactor.net.SocketSettings;
import io.activej.reactor.nio.NioReactor;
import io.activej.serializer.BinarySerializer;

import java.net.InetSocketAddress;
import java.time.Duration;

import static io.activej.crdt.util.Utils.onItem;
import static io.activej.reactor.Reactive.checkInReactorThread;

@SuppressWarnings("rawtypes")
public final class RemoteCrdtStorage<K extends Comparable<K>, S> extends AbstractNioReactive
		implements ICrdtStorage<K, S>, ReactiveService, ReactiveJmxBeanWithStats {
	public static final Duration DEFAULT_CONNECT_TIMEOUT = ApplicationSettings.getDuration(RemoteCrdtStorage.class, "connectTimeout", Duration.ZERO);
	public static final Duration DEFAULT_SMOOTHING_WINDOW = ApplicationSettings.getDuration(RemoteCrdtStorage.class, "smoothingWindow", Duration.ofMinutes(1));

	private static final ByteBufsCodec<CrdtResponse, CrdtRequest> SERIALIZER = ByteBufsCodec.ofStreamCodecs(
			Utils.CRDT_RESPONSE_CODEC,
			Utils.CRDT_REQUEST_CODEC
	);

	private final InetSocketAddress address;
	private final CrdtDataBinarySerializer<K, S> serializer;
	private final BinarySerializer<CrdtTombstone<K>> tombstoneSerializer;

	private long connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT.toMillis();
	private SocketSettings socketSettings = SocketSettings.createDefault();

	// region JMX
	private boolean detailedStats;

	private final BasicStreamStats<CrdtData<K, S>> uploadStats = StreamStats.basic();
	private final DetailedStreamStats<CrdtData<K, S>> uploadStatsDetailed = StreamStats.detailed();
	private final BasicStreamStats<CrdtData<K, S>> downloadStats = StreamStats.basic();
	private final DetailedStreamStats<CrdtData<K, S>> downloadStatsDetailed = StreamStats.detailed();
	private final BasicStreamStats<CrdtData<K, S>> takeStats = StreamStats.basic();
	private final DetailedStreamStats<CrdtData<K, S>> takeStatsDetailed = StreamStats.detailed();
	private final BasicStreamStats<CrdtTombstone<K>> removeStats = StreamStats.basic();
	private final DetailedStreamStats<CrdtTombstone<K>> removeStatsDetailed = StreamStats.detailed();

	private final EventStats uploadedItems = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats downloadedItems = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats takenItems = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats removedItems = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	// endregion

	private RemoteCrdtStorage(NioReactor reactor, InetSocketAddress address, CrdtDataBinarySerializer<K, S> serializer) {
		super(reactor);
		this.address = address;
		this.serializer = serializer;

		tombstoneSerializer = serializer.getTombstoneSerializer();
	}

	public static <K extends Comparable<K>, S> RemoteCrdtStorage<K, S> create(
			NioReactor reactor,
			InetSocketAddress address,
			CrdtDataBinarySerializer<K, S> serializer
	) {
		return builder(reactor, address, serializer).build();
	}

	public static <K extends Comparable<K>, S> RemoteCrdtStorage<K, S> create(
			NioReactor reactor,
			InetSocketAddress address,
			BinarySerializer<K> keySerializer,
			BinarySerializer<S> stateSerializer
	) {
		return builder(reactor, address, keySerializer, stateSerializer).build();
	}

	public static <K extends Comparable<K>, S> RemoteCrdtStorage<K, S>.Builder builder(
			NioReactor reactor,
			InetSocketAddress address,
			CrdtDataBinarySerializer<K, S> serializer
	) {
		return new RemoteCrdtStorage<>(reactor, address, serializer).new Builder();
	}

	public static <K extends Comparable<K>, S> RemoteCrdtStorage<K, S>.Builder builder(
			NioReactor reactor,
			InetSocketAddress address,
			BinarySerializer<K> keySerializer,
			BinarySerializer<S> stateSerializer
	) {
		CrdtDataBinarySerializer<K, S> serializer = new CrdtDataBinarySerializer<>(keySerializer, stateSerializer);
		return new RemoteCrdtStorage<>(reactor, address, serializer).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, RemoteCrdtStorage<K, S>> {
		private Builder() {}

		public Builder withConnectTimeout(Duration connectTimeout) {
			checkNotBuilt(this);
			RemoteCrdtStorage.this.connectTimeoutMillis = connectTimeout.toMillis();
			return this;
		}

		public Builder withSocketSettings(SocketSettings socketSettings) {
			checkNotBuilt(this);
			RemoteCrdtStorage.this.socketSettings = socketSettings;
			return this;
		}

		@Override
		protected RemoteCrdtStorage<K, S> doBuild() {
			return RemoteCrdtStorage.this;
		}
	}

	public InetSocketAddress getAddress() {
		return address;
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		checkInReactorThread(this);
		return connect()
				.then(RemoteCrdtStorage::performHandshake)
				.then(messaging -> messaging.send(new CrdtRequest.Upload())
						.mapException(e -> new CrdtException("Failed to send 'Upload' request", e))
						.map($ -> messaging.sendBinaryStream()
								.withAcknowledgement(ack -> ack
										.then(messaging::receive)
										.whenResult(validateFn(CrdtResponse.UploadAck.class))
										.toVoid()))
						.map(consumer -> StreamConsumer.<CrdtData<K, S>>ofSupplier(supplier ->
										supplier.transformWith(detailedStats ? uploadStatsDetailed : uploadStats)
												.transformWith(onItem(uploadedItems::recordEvent))
												.transformWith(ChannelSerializer.create(serializer))
												.streamTo(consumer))
								.withAcknowledgement(ack -> ack
										.mapException(e -> new CrdtException("Upload failed", e)))));
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp) {
		checkInReactorThread(this);
		return connect()
				.then(RemoteCrdtStorage::performHandshake)
				.then(messaging -> messaging.send(new CrdtRequest.Download(timestamp))
						.mapException(e -> new CrdtException("Failed to send 'Download' request", e))
						.then(() -> messaging.receive()
								.mapException(e -> new CrdtException("Failed to receive response", e)))
						.whenResult(validateFn(CrdtResponse.DownloadStarted.class))
						.map($ ->
								messaging.receiveBinaryStream()
										.transformWith(ChannelDeserializer.create(serializer))
										.transformWith(detailedStats ? downloadStatsDetailed : downloadStats)
										.transformWith(onItem(downloadedItems::recordEvent))
										.withEndOfStream(eos -> eos
												.then(messaging::sendEndOfStream)
												.mapException(e -> new CrdtException("Download failed", e))
												.whenResult(messaging::close)
												.whenException(messaging::closeEx))));
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> take() {
		checkInReactorThread(this);
		return connect()
				.then(RemoteCrdtStorage::performHandshake)
				.then(messaging -> messaging.send(new CrdtRequest.Take())
						.mapException(e -> new CrdtException("Failed to send 'Take' request", e))
						.then(() -> messaging.receive()
								.mapException(e -> new CrdtException("Failed to receive response", e)))
						.whenResult(validateFn(CrdtResponse.TakeStarted.class))
						.map($ -> {
							StreamSupplier<CrdtData<K, S>> supplier = messaging.receiveBinaryStream()
									.transformWith(ChannelDeserializer.create(serializer))
									.transformWith(detailedStats ? takeStatsDetailed : takeStats)
									.transformWith(onItem(takenItems::recordEvent));
							supplier.getAcknowledgement()
									.then(() -> messaging.send(new CrdtRequest.TakeAck()))
									.then(messaging::sendEndOfStream)
									.mapException(e -> new CrdtException("Take failed", e))
									.whenResult(messaging::close)
									.whenException(messaging::closeEx);
							return supplier;
						}));
	}

	@Override
	public Promise<StreamConsumer<CrdtTombstone<K>>> remove() {
		checkInReactorThread(this);
		return connect()
				.then(RemoteCrdtStorage::performHandshake)
				.then(messaging -> messaging.send(new CrdtRequest.Remove())
						.mapException(e -> new CrdtException("Failed to send 'Remove' request", e))
						.map($ -> {
							ChannelConsumer<ByteBuf> consumer = messaging.sendBinaryStream()
									.withAcknowledgement(ack -> ack
											.then(messaging::receive)
											.whenResult(validateFn(CrdtResponse.RemoveAck.class))
											.toVoid());
							return StreamConsumer.<CrdtTombstone<K>>ofSupplier(supplier ->
											supplier.transformWith(detailedStats ? removeStatsDetailed : removeStats)
													.transformWith(onItem(removedItems::recordEvent))
													.transformWith(ChannelSerializer.create(tombstoneSerializer))
													.streamTo(consumer))
									.withAcknowledgement(ack -> ack
											.mapException(e -> new CrdtException("Remove operation failed", e)));
						}));
	}

	@Override
	public Promise<Void> ping() {
		checkInReactorThread(this);
		return connect()
				.then(RemoteCrdtStorage::performHandshake)
				.then(messaging -> messaging.send(new CrdtRequest.Ping())
						.mapException(e -> new CrdtException("Failed to send 'Ping'", e))
						.then(() -> messaging.receive()
								.mapException(e -> new CrdtException("Failed to receive 'Pong'", e)))
						.whenResult(validateFn(CrdtResponse.Pong.class))
						.toVoid()
						.whenResult(messaging::close)
						.whenException(messaging::closeEx));
	}

	@Override
	public Promise<?> start() {
		checkInReactorThread(this);
		return ping();
	}

	@Override
	public Promise<?> stop() {
		checkInReactorThread(this);
		return Promise.complete();
	}

	private static <T extends CrdtResponse> FunctionEx<CrdtResponse, T> castFn(Class<T> expectedCls) {
		return response -> {
			if (response instanceof CrdtResponse.ServerError serverError) {
				throw new CrdtException(serverError.message());
			}
			if (response.getClass() != expectedCls) {
				throw new CrdtException("Received response " + response + " instead of " + expectedCls.getName());
			}
			//noinspection unchecked
			return (T) response;
		};
	}

	private static ConsumerEx<CrdtResponse> validateFn(Class<? extends CrdtResponse> expectedCls) {
		return response -> castFn(expectedCls).apply(response);
	}

	private Promise<Messaging<CrdtResponse, CrdtRequest>> connect() {
		return TcpSocket.connect(reactor, address, connectTimeoutMillis, socketSettings)
				.map(socket -> Messaging.create(socket, SERIALIZER))
				.mapException(e -> new CrdtException("Failed to connect to " + address, e));
	}

	private static Promise<Messaging<CrdtResponse, CrdtRequest>> performHandshake(Messaging<CrdtResponse, CrdtRequest> messaging) {
		return messaging.send(new CrdtRequest.Handshake(CrdtServer.VERSION))
				.then(messaging::receive)
				.map(castFn(CrdtResponse.Handshake.class))
				.map(handshake -> {
					CrdtResponse.HandshakeFailure handshakeFailure = handshake.handshakeFailure();
					if (handshakeFailure != null) {
						throw new CrdtException(String.format("Handshake failed: %s. Minimal allowed version: %s",
								handshakeFailure.message(), handshakeFailure.minimalVersion()));
					}
					return messaging;
				});
	}

	// region JMX
	@JmxOperation
	public void startDetailedMonitoring() {
		detailedStats = true;
	}

	@JmxOperation
	public void stopDetailedMonitoring() {
		detailedStats = false;
	}

	@JmxAttribute
	public BasicStreamStats getUploadStats() {
		return uploadStats;
	}

	@JmxAttribute
	public DetailedStreamStats getUploadStatsDetailed() {
		return uploadStatsDetailed;
	}

	@JmxAttribute
	public BasicStreamStats getDownloadStats() {
		return downloadStats;
	}

	@JmxAttribute
	public DetailedStreamStats getDownloadStatsDetailed() {
		return downloadStatsDetailed;
	}

	@JmxAttribute
	public BasicStreamStats getTakeStats() {
		return takeStats;
	}

	@JmxAttribute
	public DetailedStreamStats getTakeStatsDetailed() {
		return takeStatsDetailed;
	}

	@JmxAttribute
	public BasicStreamStats getRemoveStats() {
		return removeStats;
	}

	@JmxAttribute
	public DetailedStreamStats getRemoveStatsDetailed() {
		return removeStatsDetailed;
	}

	@JmxAttribute
	public EventStats getUploadedItems() {
		return uploadedItems;
	}

	@JmxAttribute
	public EventStats getDownloadedItems() {
		return downloadedItems;
	}

	@JmxAttribute
	public EventStats getTakenItems() {
		return takenItems;
	}

	@JmxAttribute
	public EventStats getRemovedItems() {
		return removedItems;
	}
	// endregion
}
