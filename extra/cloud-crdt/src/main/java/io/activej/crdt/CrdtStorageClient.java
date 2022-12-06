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

import io.activej.async.service.EventloopService;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.ApplicationSettings;
import io.activej.common.function.ConsumerEx;
import io.activej.common.initializer.WithInitializer;
import io.activej.crdt.CrdtMessagingProto.CrdtRequest;
import io.activej.crdt.CrdtMessagingProto.CrdtRequest.*;
import io.activej.crdt.CrdtMessagingProto.CrdtResponse.Handshake.NotOk;
import io.activej.crdt.CrdtMessagingProto.CrdtResponse.ResponseCase;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.stats.StreamStats;
import io.activej.datastream.stats.StreamStatsBasic;
import io.activej.datastream.stats.StreamStatsDetailed;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanWithStats;
import io.activej.eventloop.net.SocketSettings;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.stats.EventStats;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.promise.Promise;
import io.activej.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.time.Duration;

import static io.activej.crdt.CrdtMessagingProto.CrdtRequest.RequestCase.*;
import static io.activej.crdt.CrdtMessagingProto.CrdtResponse;
import static io.activej.crdt.CrdtMessagingProto.CrdtResponse.ResponseCase.*;
import static io.activej.crdt.util.Utils.codec;
import static io.activej.crdt.util.Utils.onItem;

@SuppressWarnings("rawtypes")
public final class CrdtStorageClient<K extends Comparable<K>, S> implements CrdtStorage<K, S>, EventloopService,
		EventloopJmxBeanWithStats, WithInitializer<CrdtStorageClient<K, S>> {
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.createDefault();
	public static final Duration DEFAULT_CONNECT_TIMEOUT = ApplicationSettings.getDuration(CrdtStorageClient.class, "connectTimeout", Duration.ZERO);
	public static final Duration DEFAULT_SMOOTHING_WINDOW = ApplicationSettings.getDuration(CrdtStorageClient.class, "smoothingWindow", Duration.ofMinutes(1));

	private static final ByteBufsCodec<CrdtResponse, CrdtRequest> SERIALIZER = codec(CrdtResponse.parser());

	private final Eventloop eventloop;
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
	private CrdtStorageClient(Eventloop eventloop, InetSocketAddress address, CrdtDataSerializer<K, S> serializer) {
		this.eventloop = eventloop;
		this.address = address;
		this.serializer = serializer;

		tombstoneSerializer = serializer.getTombstoneSerializer();
	}

	public static <K extends Comparable<K>, S> CrdtStorageClient<K, S> create(Eventloop eventloop, InetSocketAddress address, CrdtDataSerializer<K, S> serializer) {
		return new CrdtStorageClient<>(eventloop, address, serializer);
	}

	public static <K extends Comparable<K>, S> CrdtStorageClient<K, S> create(Eventloop eventloop, InetSocketAddress address, BinarySerializer<K> keySerializer, BinarySerializer<S> stateSerializer) {
		return new CrdtStorageClient<>(eventloop, address, new CrdtDataSerializer<>(keySerializer, stateSerializer));
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

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	public InetSocketAddress getAddress() {
		return address;
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		return connect()
				.then(CrdtStorageClient::performHandshake)
				.then(messaging -> messaging.send(request(UPLOAD))
						.mapException(e -> new CrdtException("Failed to send 'Upload' request", e))
						.map($ -> {
							ChannelConsumer<ByteBuf> consumer = messaging.sendBinaryStream()
									.withAcknowledgement(ack -> ack
											.then(messaging::receive)
											.whenResult(simpleHandlerFn(UPLOAD_ACK))
											.toVoid());
							return StreamConsumer.<CrdtData<K, S>>ofSupplier(supplier ->
											supplier.transformWith(detailedStats ? uploadStatsDetailed : uploadStats)
													.transformWith(onItem(uploadedItems::recordEvent))
													.transformWith(ChannelSerializer.create(serializer))
													.streamTo(consumer))
									.withAcknowledgement(ack -> ack
											.mapException(e -> new CrdtException("Upload failed", e)));
						}));
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp) {
		return connect()
				.then(CrdtStorageClient::performHandshake)
				.then(messaging -> messaging.send(downloadRequest(timestamp))
						.mapException(e -> new CrdtException("Failed to send 'Download' request", e))
						.then(() -> messaging.receive()
								.mapException(e -> new CrdtException("Failed to receive response", e)))
						.whenResult(simpleHandlerFn(DOWNLOAD_STARTED))
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
				.then(messaging -> messaging.send(request(TAKE))
						.mapException(e -> new CrdtException("Failed to send 'Take' request", e))
						.then(() -> messaging.receive()
								.mapException(e -> new CrdtException("Failed to receive response", e)))
						.whenResult(simpleHandlerFn(TAKE_STARTED))
						.map($ -> {
							StreamSupplier<CrdtData<K, S>> supplier = messaging.receiveBinaryStream()
									.transformWith(ChannelDeserializer.create(serializer))
									.transformWith(detailedStats ? takeStatsDetailed : takeStats)
									.transformWith(onItem(takenItems::recordEvent));
							supplier.getAcknowledgement()
									.then(() -> messaging.send(request(TAKE_ACK)))
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
				.then(messaging -> messaging.send(request(REMOVE))
						.mapException(e -> new CrdtException("Failed to send 'Remove' request", e))
						.map($ -> {
							ChannelConsumer<ByteBuf> consumer = messaging.sendBinaryStream()
									.withAcknowledgement(ack -> ack
											.then(messaging::receive)
											.whenResult(simpleHandlerFn(REMOVE_ACK))
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
				.then(messaging -> messaging.send(request(PING))
						.mapException(e -> new CrdtException("Failed to send 'Ping'", e))
						.then(() -> messaging.receive()
								.mapException(e -> new CrdtException("Failed to receive 'Pong'", e)))
						.whenResult(simpleHandlerFn(PONG))
						.toVoid()
						.whenResult(messaging::close)
						.whenException(messaging::closeEx));
	}

	@Override
	public @NotNull Promise<Void> start() {
		return ping();
	}

	@Override
	public @NotNull Promise<Void> stop() {
		return Promise.complete();
	}

	private ConsumerEx<CrdtResponse> simpleHandlerFn(ResponseCase responseCase) {
		return response -> {
			ResponseCase actualCase = response.getResponseCase();
			if (actualCase == SERVER_ERROR) {
				throw new CrdtException((response.getServerError()).getMessage());
			}
			if (actualCase == RESPONSE_NOT_SET) {
				throw new CrdtException("Received empty request");
			}
			if (actualCase != responseCase) {
				throw new CrdtException("Received request " + actualCase + " instead of " + responseCase);
			}
		};
	}

	private Promise<MessagingWithBinaryStreaming<CrdtResponse, CrdtRequest>> connect() {
		return AsyncTcpSocketNio.connect(address, connectTimeoutMillis, socketSettings)
				.map(socket -> MessagingWithBinaryStreaming.create(socket, SERIALIZER))
				.mapException(e -> new CrdtException("Failed to connect to " + address, e));
	}

	private static CrdtRequest request(RequestCase requestCase) {
		CrdtRequest.Builder builder = CrdtRequest.newBuilder();
		return switch (requestCase) {
			case UPLOAD -> builder.setUpload(Upload.newBuilder()).build();
			case REMOVE -> builder.setRemove(Remove.newBuilder()).build();
			case PING -> builder.setPing(Ping.newBuilder()).build();
			case TAKE -> builder.setTake(Take.newBuilder()).build();
			case TAKE_ACK -> builder.setTakeAck(TakeAck.newBuilder()).build();
			default -> throw new AssertionError();
		};

	}

	private static Promise<MessagingWithBinaryStreaming<CrdtResponse, CrdtRequest>> performHandshake(MessagingWithBinaryStreaming<CrdtResponse, CrdtRequest> messaging) {
		return messaging.send(CrdtRequest.newBuilder().setHandshake(CrdtRequest.Handshake.newBuilder().setVersion(CrdtServer.VERSION)).build())
				.then(messaging::receive)
				.map(handshakeResponse -> {
					if (!handshakeResponse.hasHandshake()) {
						if (handshakeResponse.hasServerError()) {
							throw new CrdtException(handshakeResponse.getServerError().getMessage());
						}
						throw new CrdtException("Handshake response expected, got " + handshakeResponse.getResponseCase());
					}
					CrdtResponse.Handshake handshake = handshakeResponse.getHandshake();
					if (handshake.hasNotOk()) {
						NotOk notOk = handshake.getNotOk();
						throw new CrdtException(String.format("Handshake failed: %s. Minimal allowed version: %s",
								notOk.getMessage(), notOk.hasMinimalVersion() ? notOk.getMinimalVersion() : "unspecified"));
					}
					return messaging;
				});
	}

	private static CrdtRequest downloadRequest(long timestamp) {
		return CrdtRequest.newBuilder()
				.setDownload(Download.newBuilder()
						.setToken(timestamp))
				.build();
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
