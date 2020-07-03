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

package io.activej.remotefs;

import io.activej.async.service.EventloopService;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.exception.StacklessException;
import io.activej.common.ref.RefLong;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.eventloop.net.SocketSettings;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import io.activej.remotefs.RemoteFsCommands.*;
import io.activej.remotefs.RemoteFsResponses.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Preconditions.checkArgument;
import static io.activej.common.collection.CollectionUtils.toLimitedString;
import static io.activej.csp.binary.BinaryChannelSupplier.UNEXPECTED_END_OF_STREAM_EXCEPTION;
import static io.activej.remotefs.util.RemoteFsUtils.ID_TO_ERROR;
import static io.activej.remotefs.util.RemoteFsUtils.nullTerminatedJson;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;

/**
 * An implementation of {@link FsClient} which connects to a single {@link RemoteFsServer} and communicates with it.
 */
public final class RemoteFsClient implements FsClient, EventloopService, EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsClient.class);

	public static final StacklessException INVALID_MESSAGE = new StacklessException(RemoteFsClient.class, "Invalid or unexpected message received");
	public static final StacklessException TOO_MUCH_DATA = new StacklessException(RemoteFsClient.class, "Received more bytes than expected");
	public static final StacklessException UNKNOWN_SERVER_ERROR = new StacklessException(RemoteFsClient.class, "Unknown server error occurred");

	private static final ByteBufsCodec<FsResponse, FsCommand> SERIALIZER =
			nullTerminatedJson(RemoteFsResponses.CODEC, RemoteFsCommands.CODEC);

	private final Eventloop eventloop;
	private final InetSocketAddress address;

	private SocketSettings socketSettings = SocketSettings.createDefault();

	//region JMX
	private final PromiseStats connectPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadStartPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadStartPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats copyPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats copyAllPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats movePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats moveAllPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats listPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats infoPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats infoAllPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats pingPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deletePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deleteAllPromise = PromiseStats.create(Duration.ofMinutes(5));
	//endregion

	// region creators
	private RemoteFsClient(Eventloop eventloop, InetSocketAddress address) {
		this.eventloop = eventloop;
		this.address = address;
	}

	public static RemoteFsClient create(Eventloop eventloop, InetSocketAddress address) {
		return new RemoteFsClient(eventloop, address);
	}

	public RemoteFsClient withSocketSettings(SocketSettings socketSettings) {
		this.socketSettings = socketSettings;
		return this;
	}
	// endregion

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String filename) {
		return connect(address)
				.then(messaging ->
						messaging.send(new Upload(filename))
								.then(messaging::receive)
								.then(msg -> cast(msg, UploadAck.class))
								.then(() -> Promise.of(messaging.sendBinaryStream()
										.withAcknowledgement(ack -> ack
												.then(messaging::receive)
												.whenResult(messaging::close)
												.then(msg -> cast(msg, UploadFinished.class).toVoid())
												.whenException(e -> {
													messaging.closeEx(e);
													logger.warn("Cancelled while trying to upload file {} : {}", filename, this, e);
												})
												.whenComplete(uploadFinishPromise.recordStats()))))
								.whenException(e -> {
									messaging.closeEx(e);
									logger.warn("Error while trying to upload file {} : {}", filename, this, e);
								}))
				.whenComplete(toLogger(logger, "upload", filename, this))
				.whenComplete(uploadStartPromise.recordStats());
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long limit) {
		checkArgument(offset >= 0, "Data offset must be greater than or equal to zero");
		checkArgument(limit >= 0, "Data limit must be greater than or equal to zero");

		return connect(address)
				.then(messaging ->
						messaging.send(new Download(name, offset, limit))
								.then(messaging::receive)
								.then(msg -> cast(msg, DownloadSize.class))
								.then(msg -> {
									long receivingSize = msg.getSize();
									if (receivingSize > limit) {
										return Promise.ofException(TOO_MUCH_DATA);
									}

									logger.trace("download size for file {} is {}: {}", name, receivingSize, this);

									RefLong size = new RefLong(0);
									return Promise.of(messaging.receiveBinaryStream()
											.peek(buf -> size.inc(buf.readRemaining()))
											.withEndOfStream(eos -> eos
													.then(messaging::sendEndOfStream)
													.then(() -> {
														if (size.get() == receivingSize) {
															return Promise.complete();
														}
														logger.error("invalid stream size for file " + name +
																" (offset " + offset + ", limit " + limit + ")," +
																" expected: " + receivingSize +
																" actual: " + size.get());
														return Promise.ofException(size.get() < receivingSize ? UNEXPECTED_END_OF_STREAM_EXCEPTION : TOO_MUCH_DATA);
													})
													.whenComplete(downloadFinishPromise.recordStats())
													.whenResult(messaging::close)));
								})
								.whenException(e -> {
									messaging.closeEx(e);
									logger.warn("error trying to download file {} (offset={}, limit={}) : {}", name, offset, limit, this, e);
								}))
				.whenComplete(toLogger(logger, "download", name, offset, limit, this))
				.whenComplete(downloadStartPromise.recordStats());
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target) {
		return simpleCommand(new Copy(name, target), CopyFinished.class, $ -> (Void) null)
				.whenComplete(toLogger(logger, "copy", name, target, this))
				.whenComplete(copyPromise.recordStats());
	}

	@Override
	public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return simpleCommand(new CopyAll(sourceToTarget), CopyAllFinished.class, $ -> (Void) null)
				.whenComplete(toLogger(logger, "copyAll", toLimitedString(sourceToTarget, 50), this))
				.whenComplete(copyAllPromise.recordStats());
	}

	@Override
	public Promise<Void> move(@NotNull String name, @NotNull String target) {
		return simpleCommand(new Move(name, target), MoveFinished.class, $ -> (Void) null)
				.whenComplete(toLogger(logger, "move", name, target, this))
				.whenComplete(movePromise.recordStats());
	}

	@Override
	public Promise<Void> moveAll(Map<String, String> sourceToTarget) {
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return simpleCommand(new MoveAll(sourceToTarget), MoveAllFinished.class, $ -> (Void) null)
				.whenComplete(toLogger(logger, "moveAll", toLimitedString(sourceToTarget, 50), this))
				.whenComplete(moveAllPromise.recordStats());
	}

	@Override
	public Promise<Void> delete(@NotNull String name) {
		return simpleCommand(new Delete(name), DeleteFinished.class, $ -> (Void) null)
				.whenComplete(toLogger(logger, "delete", name, this))
				.whenComplete(deletePromise.recordStats());
	}

	@Override
	public Promise<Void> deleteAll(Set<String> toDelete) {
		if (toDelete.isEmpty()) return Promise.complete();

		return simpleCommand(new DeleteAll(toDelete), DeleteAllFinished.class, $ -> (Void) null)
				.whenComplete(toLogger(logger, "deleteAll", toLimitedString(toDelete, 100), this))
				.whenComplete(deleteAllPromise.recordStats());
	}

	@Override
	public Promise<List<FileMetadata>> list(@NotNull String glob) {
		return simpleCommand(new RemoteFsCommands.List(glob), ListFinished.class, ListFinished::getFiles)
				.whenComplete(toLogger(logger, "list", glob, this))
				.whenComplete(listPromise.recordStats());
	}

	@Override
	public Promise<@Nullable FileMetadata> info(@NotNull String name) {
		return simpleCommand(new Inspect(name), InspectFinished.class, InspectFinished::getMetadata)
				.whenComplete(toLogger(logger, "info", name, this))
				.whenComplete(infoPromise.recordStats());
	}

	@Override
	public Promise<Map<String, @Nullable FileMetadata>> infoAll(@NotNull List<String> names) {
		if (names.isEmpty()) return Promise.of(emptyMap());

		return simpleCommand(new InspectAll(names), InspectAllFinished.class,
				response -> {
					Map<String, FileMetadata> result = response.getMetadataList().stream()
							.collect(toMap(FileMetadata::getName, Function.identity()));
					names.forEach(name -> result.putIfAbsent(name, null));
					return result;
				})
				.whenComplete(toLogger(logger, "infoAll", toLimitedString(names, 100), this))
				.whenComplete(infoAllPromise.recordStats());
	}

	@Override
	public Promise<Void> ping() {
		return simpleCommand(new Ping(), PingFinished.class, $ -> (Void) null)
				.whenComplete(toLogger(logger, "ping", this))
				.whenComplete(pingPromise.recordStats());
	}

	private Promise<MessagingWithBinaryStreaming<FsResponse, FsCommand>> connect(InetSocketAddress address) {
		return AsyncTcpSocketNio.connect(address, 0, socketSettings)
				.map(socket -> MessagingWithBinaryStreaming.create(socket, SERIALIZER))
				.whenResult(() -> logger.trace("connected to [{}]: {}", address, this))
				.whenException(e -> logger.warn("failed connecting to [{}] : {}", address, this, e))
				.whenComplete(connectPromise.recordStats());
	}

	private static <T extends FsResponse> Promise<T> cast(FsResponse msg, Class<T> expectedClass) {
		if (expectedClass == msg.getClass()) {
			return Promise.of(expectedClass.cast(msg));
		}
		if (msg instanceof ServerError) {
			int code = ((ServerError) msg).getCode();
			Throwable error = ID_TO_ERROR.getOrDefault(code, UNKNOWN_SERVER_ERROR);
			return Promise.ofException(error);
		}
		return Promise.ofException(INVALID_MESSAGE);
	}

	private <T, R extends FsResponse> Promise<T> simpleCommand(FsCommand command, Class<R> responseType, Function<R, T> answerExtractor) {
		return connect(address)
				.then(messaging ->
						messaging.send(command)
								.then(messaging::receive)
								.whenResult(messaging::close)
								.then(msg -> cast(msg, responseType))
								.map(answerExtractor)
								.whenException(e -> {
									messaging.closeEx(e);
									logger.warn("Error while processing command {} : {}", command, this, e);
								}));
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

	@Override
	public String toString() {
		return "RemoteFsClient{address=" + address + '}';
	}

	//region JMX
	@JmxAttribute
	public PromiseStats getConnectPromise() {
		return connectPromise;
	}

	@JmxAttribute
	public PromiseStats getUploadStartPromise() {
		return uploadStartPromise;
	}

	@JmxAttribute
	public PromiseStats getUploadFinishPromise() {
		return uploadFinishPromise;
	}

	@JmxAttribute
	public PromiseStats getDownloadStartPromise() {
		return downloadStartPromise;
	}

	@JmxAttribute
	public PromiseStats getDownloadFinishPromise() {
		return downloadFinishPromise;
	}

	@JmxAttribute
	public PromiseStats getCopyPromise() {
		return copyPromise;
	}

	@JmxAttribute
	public PromiseStats getCopyAllPromise() {
		return copyAllPromise;
	}

	@JmxAttribute
	public PromiseStats getMovePromise() {
		return movePromise;
	}

	@JmxAttribute
	public PromiseStats getMoveAllPromise() {
		return moveAllPromise;
	}

	@JmxAttribute
	public PromiseStats getListPromise() {
		return listPromise;
	}

	@JmxAttribute
	public PromiseStats getInfoPromise() {
		return infoPromise;
	}

	@JmxAttribute
	public PromiseStats getInfoAllPromise() {
		return infoAllPromise;
	}

	@JmxAttribute
	public PromiseStats getPingPromise() {
		return pingPromise;
	}

	@JmxAttribute
	public PromiseStats getDeletePromise() {
		return deletePromise;
	}

	@JmxAttribute
	public PromiseStats getDeleteAllPromise() {
		return deleteAllPromise;
	}
	//endregion
}
