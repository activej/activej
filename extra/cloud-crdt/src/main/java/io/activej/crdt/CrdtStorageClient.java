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
import io.activej.common.function.ConsumerEx;
import io.activej.common.function.FunctionEx;
import io.activej.common.initializer.WithInitializer;
import io.activej.crdt.messaging.CrdtRequest;
import io.activej.crdt.messaging.CrdtResponse;
import io.activej.crdt.storage.AsyncCrdtStorage;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.crdt.util.Utils;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.net.MessagingCodec;
import io.activej.csp.net.ReactiveMessaging;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.stats.StreamStats;
import io.activej.datastream.stats.StreamStatsBasic;
import io.activej.datastream.stats.StreamStatsDetailed;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.stats.EventStats;
import io.activej.net.socket.tcp.ReactiveTcpSocket;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractNioReactive;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;
import io.activej.reactor.net.SocketSettings;
import io.activej.reactor.nio.NioReactor;
import io.activej.serializer.BinarySerializer;

import java.net.InetSocketAddress;
import java.time.Duration;

import static io.activej.crdt.util.Utils.onItem;

@SuppressWarnings("rawtypes")
public final class CrdtStorageClient<K extends Comparable<K>, S> extends AbstractNioReactive
		implements AsyncCrdtStorage<K, S>, ReactiveService, ReactiveJmxBeanWithStats, WithInitializer<CrdtStorageClient<K, S>> {
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.createDefault();
	public static final Duration DEFAULT_CONNECT_TIMEOUT = ApplicationSettings.getDuration(CrdtStorageClient.class, "connectTimeout", Duration.ZERO);
	public static final Duration DEFAULT_SMOOTHING_WINDOW = ApplicationSettings.getDuration(CrdtStorageClient.class, "smoothingWindow", Duration.ofMinutes(1));

	private static final ByteBufsCodec<CrdtResponse, CrdtRequest> SERIALIZER = MessagingCodec.create(
			Utils.CRDT_RESPONSE_CODEC,
			Utils.CRDT_REQUEST_CODEC
	);

	private final InetSocketAddress address;
	private final CrdtDataSerializer<K, S> serializer;
	private final BinarySerializer<CrdtTombstone<K>> tombstoneSerializer;

	private long connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT.toMillis();
	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;

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

	private final EventStats uploadedItems = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats downloadedItems = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats takenItems = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats removedItems = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	// endregion

	//region creators
	private CrdtStorageClient(NioReactor reactor, InetSocketAddress address, CrdtDataSerializer<K, S> serializer) {
		super(reactor);
		this.address = address;
		this.serializer = serializer;

		tombstoneSerializer = serializer.getTombstoneSerializer();
	}

	public static <K extends Comparable<K>, S> CrdtStorageClient<K, S> create(NioReactor reactor, InetSocketAddress address, CrdtDataSerializer<K, S> serializer) {
		return new CrdtStorageClient<>(reactor, address, serializer);
	}

	public static <K extends Comparable<K>, S> CrdtStorageClient<K, S> create(NioReactor reactor, InetSocketAddress address, BinarySerializer<K> keySerializer, BinarySerializer<S> stateSerializer) {
		return new CrdtStorageClient<>(reactor, address, new CrdtDataSerializer<>(keySerializer, stateSerializer));
	}

	public CrdtStorageClient<K, S> withConnectTimeout(Duration connectTimeout) {
		this.connectTimeoutMillis = connectTimeout.toMillis();
		return this;
	}

	public CrdtStorageClient<K, S> withSocketSettings(SocketSettings socketSettings) {
		this.socketSettings = socketSettings;
		return this;
	}
	//endregion

	public InetSocketAddress getAddress() {
		return address;
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		return connect()
				.then(CrdtStorageClient::performHandshake)
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
		return connect()
				.then(CrdtStorageClient::performHandshake)
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
		return connect()
				.then(CrdtStorageClient::performHandshake)
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
		return connect()
				.then(CrdtStorageClient::performHandshake)
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
		return connect()
				.then(CrdtStorageClient::performHandshake)
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
		return ping();
	}

	@Override
	public Promise<?> stop() {
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

	private Promise<ReactiveMessaging<CrdtResponse, CrdtRequest>> connect() {
		return ReactiveTcpSocket.connect(reactor, address, connectTimeoutMillis, socketSettings)
				.map(socket -> ReactiveMessaging.create(socket, SERIALIZER))
				.mapException(e -> new CrdtException("Failed to connect to " + address, e));
	}

	private static Promise<ReactiveMessaging<CrdtResponse, CrdtRequest>> performHandshake(ReactiveMessaging<CrdtResponse, CrdtRequest> messaging) {
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
