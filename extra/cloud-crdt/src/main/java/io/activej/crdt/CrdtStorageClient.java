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
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.promise.Promise;
import io.activej.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.time.Duration;

import static io.activej.crdt.CrdtMessaging.*;
import static io.activej.crdt.CrdtMessaging.CrdtMessages.PING;
import static io.activej.crdt.CrdtMessaging.CrdtResponses.*;
import static io.activej.crdt.util.Utils.*;
import static io.activej.csp.binary.Utils.nullTerminated;

@SuppressWarnings("rawtypes")
public final class CrdtStorageClient<K extends Comparable<K>, S> implements CrdtStorage<K, S>, EventloopService, EventloopJmxBeanWithStats {
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.createDefault();
	public static final Duration DEFAULT_CONNECT_TIMEOUT = ApplicationSettings.getDuration(CrdtStorageClient.class, "connectTimeout", Duration.ZERO);

	private static final ByteBufsCodec<CrdtResponse, CrdtMessage> SERIALIZER =
			nullTerminated()
					.andThen(
							value -> {
								try {
									return fromJson(CrdtResponse.class, value);
								} finally {
									value.recycle();
								}
							},
							crdtMessage -> toJson(CrdtMessage.class, crdtMessage));

	private final Eventloop eventloop;
	private final InetSocketAddress address;
	private final CrdtDataSerializer<K, S> serializer;
	private final BinarySerializer<K> keySerializer;

	private long connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT.toMillis();
	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;

	// region JMX
	private boolean detailedStats;

	private final StreamStatsBasic<CrdtData<K, S>> uploadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> uploadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<CrdtData<K, S>> downloadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> downloadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<K> removeStats = StreamStats.basic();
	private final StreamStatsDetailed<K> removeStatsDetailed = StreamStats.detailed();
	// endregion

	//region creators
	private CrdtStorageClient(Eventloop eventloop, InetSocketAddress address, CrdtDataSerializer<K, S> serializer) {
		this.eventloop = eventloop;
		this.address = address;
		this.serializer = serializer;

		keySerializer = serializer.getKeySerializer();
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

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		return connect()
				.then(messaging ->
						messaging.send(CrdtMessages.UPLOAD)
								.then(wrapException(() -> "Failed to send 'Upload' message"))
								.map($ -> {
									ChannelConsumer<ByteBuf> consumer = messaging.sendBinaryStream()
											.withAcknowledgement(ack -> ack
													.then(messaging::receive)
													.whenResult(simpleHandlerFn(UPLOAD_FINISHED))
													.toVoid());
									return StreamConsumer.<CrdtData<K, S>>ofSupplier(supplier ->
											supplier.transformWith(detailedStats ? uploadStatsDetailed : uploadStats)
													.transformWith(ChannelSerializer.create(serializer))
													.streamTo(consumer))
											.withAcknowledgement(ack -> ack
													.then(wrapException(() -> "Upload failed")));
								}));
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp) {
		return connect()
				.then(messaging -> messaging.send(new Download(timestamp))
						.then(wrapException(() -> "Failed to send 'Download' message"))
						.then(() -> messaging.receive()
								.then(wrapException(() -> "Failed to receive response")))
						.whenResult(response -> {
							if (response == DOWNLOAD_STARTED) {
								return;
							}
							if (response.getClass() == ServerError.class) {
								throw new CrdtException(((ServerError) response).getMsg());
							}
							throw new CrdtException("Received message " + response + " instead of " + DOWNLOAD_STARTED);
						})
						.map($ ->
								messaging.receiveBinaryStream()
										.transformWith(ChannelDeserializer.create(serializer))
										.transformWith(detailedStats ? downloadStatsDetailed : downloadStats)
										.withEndOfStream(eos -> eos
												.then(messaging::sendEndOfStream)
												.then(wrapException(() -> "Download failed"))
												.whenComplete(($2, e) -> {
													if (e == null) {
														messaging.close();
													} else {
														messaging.closeEx(e);
													}
												}))));
	}

	@Override
	public Promise<StreamConsumer<K>> remove() {
		return connect()
				.then(messaging ->
						messaging.send(CrdtMessages.REMOVE)
								.then(wrapException(() -> "Failed to send 'Remove' message"))
								.map($ -> {
									ChannelConsumer<ByteBuf> consumer = messaging.sendBinaryStream()
											.withAcknowledgement(ack -> ack
													.then(messaging::receive)
													.whenResult(simpleHandlerFn(REMOVE_FINISHED))
													.toVoid());
									return StreamConsumer.<K>ofSupplier(supplier ->
											supplier.transformWith(detailedStats ? removeStatsDetailed : removeStats)
													.transformWith(ChannelSerializer.create(keySerializer))
													.streamTo(consumer))
											.withAcknowledgement(ack -> ack
													.then(wrapException(() -> "Remove operation failed")));
								}));
	}

	@Override
	public Promise<Void> ping() {
		return connect()
				.then(messaging -> messaging.send(PING)
						.then(wrapException(() -> "Failed to send 'Ping'"))
						.then(() -> messaging.receive()
								.then(wrapException(() -> "Failed to receive 'Pong'")))
						.whenResult(simpleHandlerFn(PONG))
						.toVoid()
						.whenResult(messaging::close)
						.whenException(messaging::closeEx));
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return ping();
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	private ConsumerEx<CrdtResponse> simpleHandlerFn(CrdtResponse expected) {
		return response -> {
			if (response == expected) {
				return;
			}
			if (response instanceof ServerError) {
				throw new CrdtException(((ServerError) response).getMsg());
			}
			throw new CrdtException("Received message " + response + " instead of " + expected);
		};
	}

	private Promise<MessagingWithBinaryStreaming<CrdtResponse, CrdtMessage>> connect() {
		return AsyncTcpSocketNio.connect(address, connectTimeoutMillis, socketSettings)
				.map(socket -> MessagingWithBinaryStreaming.create(socket, SERIALIZER))
				.then(wrapException(() -> "Failed to connect to " + address));
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
	public StreamStatsBasic getRemoveStats() {
		return removeStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getRemoveStatsDetailed() {
		return removeStatsDetailed;
	}
	// endregion
}
