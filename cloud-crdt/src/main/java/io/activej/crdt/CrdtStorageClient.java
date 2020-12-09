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
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.stats.StreamStats;
import io.activej.datastream.stats.StreamStatsBasic;
import io.activej.datastream.stats.StreamStatsDetailed;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.eventloop.net.SocketSettings;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.promise.Promise;
import io.activej.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.util.function.Function;

import static io.activej.crdt.CrdtMessaging.*;
import static io.activej.crdt.CrdtMessaging.CrdtMessages.PING;
import static io.activej.crdt.CrdtMessaging.CrdtResponses.*;
import static io.activej.crdt.util.Utils.nullTerminatedJson;
import static io.activej.crdt.util.Utils.wrapException;

@SuppressWarnings("rawtypes")
public final class CrdtStorageClient<K extends Comparable<K>, S> implements CrdtStorage<K, S>, EventloopService, EventloopJmxBeanEx {
	private final Eventloop eventloop;
	private final InetSocketAddress address;
	private final CrdtDataSerializer<K, S> serializer;
	private final BinarySerializer<K> keySerializer;

	private SocketSettings socketSettings = SocketSettings.createDefault();

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
								.thenEx(wrapException(() -> "Failed to send 'Upload' message"))
								.map($ -> {
									ChannelConsumer<ByteBuf> consumer = messaging.sendBinaryStream()
											.withAcknowledgement(ack -> ack
													.then(messaging::receive)
													.then(simpleHandler(UPLOAD_FINISHED)));
									return StreamConsumer.<CrdtData<K, S>>ofSupplier(supplier ->
											supplier.transformWith(detailedStats ? uploadStats : uploadStatsDetailed)
													.transformWith(ChannelSerializer.create(serializer))
													.streamTo(consumer))
											.withAcknowledgement(ack -> ack
													.thenEx(wrapException(() -> "Upload failed")));
								}));
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp) {
		return connect()
				.then(messaging -> messaging.send(new Download(timestamp))
						.thenEx(wrapException(() -> "Failed to send 'Download' message"))
						.then(() -> messaging.receive()
								.thenEx(wrapException(() -> "Failed to receive response")))
						.then(response -> {
							Class<? extends CrdtResponse> responseClass = response.getClass();
							if (responseClass == DownloadStarted.class) {
								return Promise.complete();
							}
							if (responseClass == ServerError.class) {
								return Promise.ofException(new CrdtException(((ServerError) response).getMsg()));
							}
							return Promise.ofException(new CrdtException("Received message " + response + " instead of " + DownloadStarted.class.getSimpleName()));
						})
						.map($ ->
								messaging.receiveBinaryStream()
										.transformWith(ChannelDeserializer.create(serializer))
										.transformWith(detailedStats ? downloadStats : downloadStatsDetailed)
										.withEndOfStream(eos -> eos
												.then(messaging::sendEndOfStream)
												.thenEx(wrapException(() -> "Download failed"))
												.whenResult(messaging::close))));
	}

	@Override
	public Promise<StreamConsumer<K>> remove() {
		return connect()
				.then(messaging ->
						messaging.send(CrdtMessages.REMOVE)
								.thenEx(wrapException(() -> "Failed to send 'Remove' message"))
								.map($ -> {
									ChannelConsumer<ByteBuf> consumer = messaging.sendBinaryStream()
											.withAcknowledgement(ack -> ack
													.then(messaging::receive)
													.then(simpleHandler(REMOVE_FINISHED)));
									return StreamConsumer.<K>ofSupplier(supplier ->
											supplier.transformWith(detailedStats ? removeStats : removeStatsDetailed)
													.transformWith(ChannelSerializer.create(keySerializer))
													.streamTo(consumer))
											.withAcknowledgement(ack -> ack
													.thenEx(wrapException(() -> "Remove operation failed")));
								}));
	}

	@Override
	public Promise<Void> ping() {
		return connect()
				.then(messaging -> messaging.send(PING)
						.thenEx(wrapException(() -> "Failed to send 'Ping'"))
						.then(() -> messaging.receive()
								.thenEx(wrapException(() -> "Failed to receive 'Pong'")))
						.then(simpleHandler(PONG)));
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

	private Function<CrdtResponse, Promise<Void>> simpleHandler(CrdtResponse expected) {
		return response -> {
			if (response == expected) {
				return Promise.complete();
			}
			if (response instanceof ServerError) {
				return Promise.ofException(new CrdtException(((ServerError) response).getMsg()));
			}
			return Promise.ofException(new CrdtException("Received message " + response + " instead of " + expected));
		};
	}

	private Promise<MessagingWithBinaryStreaming<CrdtResponse, CrdtMessage>> connect() {
		return AsyncTcpSocketNio.connect(address, null, socketSettings)
				.map(socket -> MessagingWithBinaryStreaming.create(socket, nullTerminatedJson(RESPONSE_CODEC, MESSAGE_CODEC)))
				.thenEx(wrapException(() -> "Failed to connect to " + address));
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
