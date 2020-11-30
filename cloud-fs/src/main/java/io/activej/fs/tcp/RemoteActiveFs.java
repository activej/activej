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

package io.activej.fs.tcp;

import io.activej.async.service.EventloopService;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.ApplicationSettings;
import io.activej.common.exception.parse.TruncatedDataException;
import io.activej.common.exception.parse.UnexpectedDataException;
import io.activej.common.ref.RefLong;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.eventloop.net.SocketSettings;
import io.activej.fs.ActiveFs;
import io.activej.fs.FileMetadata;
import io.activej.fs.exception.FsIOException;
import io.activej.fs.tcp.RemoteFsCommands.*;
import io.activej.fs.tcp.RemoteFsResponses.*;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.collection.CollectionUtils.isBijection;
import static io.activej.common.collection.CollectionUtils.toLimitedString;
import static io.activej.csp.dsl.ChannelConsumerTransformer.identity;
import static io.activej.fs.util.RemoteFsUtils.nullTerminatedJson;
import static io.activej.fs.util.RemoteFsUtils.ofFixedSize;
import static java.util.Collections.emptyMap;

/**
 * A client to the remote {@link ActiveFsServer}.
 * All client/server communication is done via TCP.
 * <p>
 * <b>This client should only be used on private networks.</b>
 * <p>
 * Inherits all of the limitations of {@link ActiveFs} implementation located on {@link ActiveFsServer}.
 */
public final class RemoteActiveFs implements ActiveFs, EventloopService, EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(RemoteActiveFs.class);

	public static final Duration DEFAULT_CONNECTION_TIMEOUT = ApplicationSettings.getDuration(RemoteActiveFs.class, "connectionTimeout", Duration.ZERO);

	private static final FsIOException INVALID_MESSAGE = new FsIOException(RemoteActiveFs.class, "Invalid or unexpected message received");
	private static final TruncatedDataException UNEXPECTED_END_OF_STREAM = new TruncatedDataException(RemoteActiveFs.class);
	private static final UnexpectedDataException UNEXPECTED_DATA = new UnexpectedDataException(RemoteActiveFs.class);

	private static final ByteBufsCodec<FsResponse, FsCommand> SERIALIZER =
			nullTerminatedJson(RemoteFsResponses.CODEC, RemoteFsCommands.CODEC);

	private final Eventloop eventloop;
	private final InetSocketAddress address;

	private SocketSettings socketSettings = SocketSettings.createDefault();
	private int connectionTimeout = (int) DEFAULT_CONNECTION_TIMEOUT.toMillis();

	//region JMX
	private final PromiseStats connectPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadStartPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats appendStartPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats appendFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
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
	private RemoteActiveFs(Eventloop eventloop, InetSocketAddress address) {
		this.eventloop = eventloop;
		this.address = address;
	}

	public static RemoteActiveFs create(Eventloop eventloop, InetSocketAddress address) {
		return new RemoteActiveFs(eventloop, address);
	}

	public RemoteActiveFs withSocketSettings(SocketSettings socketSettings) {
		this.socketSettings = socketSettings;
		return this;
	}

	public RemoteActiveFs withConnectionTimeout(Duration connectionTimeout) {
		this.connectionTimeout = (int) connectionTimeout.toMillis();
		return this;
	}
	// endregion

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name) {
		return connectForStreaming(address)
				.then(messaging -> doUpload(messaging, name, null))
				.whenComplete(uploadStartPromise.recordStats())
				.whenComplete(toLogger(logger, "upload", name, this));
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name, long size) {
		return connect(address)
				.then(messaging -> doUpload(messaging, name, size))
				.whenComplete(uploadStartPromise.recordStats())
				.whenComplete(toLogger(logger, "upload", name, size, this));
	}

	@NotNull
	private Promise<ChannelConsumer<ByteBuf>> doUpload(MessagingWithBinaryStreaming<FsResponse, FsCommand> messaging, @NotNull String name, @Nullable Long size) {
		return messaging.send(new Upload(name, size))
				.then(messaging::receive)
				.then(msg -> cast(msg, UploadAck.class))
				.then(() -> Promise.of(messaging.sendBinaryStream()
						.transformWith(size == null ? identity() : ofFixedSize(size))
						.withAcknowledgement(ack -> ack
								.then(messaging::receive)
								.whenResult(messaging::close)
								.then(msg -> cast(msg, UploadFinished.class).toVoid())
								.whenException(e -> {
									messaging.closeEx(e);
									logger.warn("Cancelled while trying to upload file {}: {}", name, this, e);
								})
								.whenComplete(uploadFinishPromise.recordStats())
								.whenComplete(toLogger(logger, TRACE, "onUploadComplete", messaging, name, size, this)))))
				.whenException(e -> {
					messaging.closeEx(e);
					logger.warn("Error while trying to upload file {}: {}", name, this, e);
				});
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> append(@NotNull String name, long offset) {
		return connect(address)
				.then(messaging ->
						messaging.send(new Append(name, offset))
								.then(messaging::receive)
								.then(msg -> cast(msg, AppendAck.class))
								.then(() -> Promise.of(messaging.sendBinaryStream()
										.withAcknowledgement(ack -> ack
												.then(messaging::receive)
												.whenResult(messaging::close)
												.then(msg -> cast(msg, AppendFinished.class).toVoid())
												.whenException(messaging::closeEx)
												.whenComplete(appendFinishPromise.recordStats())
												.whenComplete(toLogger(logger, TRACE, "onAppendComplete", name, offset, this)))))
								.whenException(messaging::closeEx))
				.whenComplete(appendStartPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, "append", name, offset, this));
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
										return Promise.ofException(UNEXPECTED_DATA);
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
														return Promise.ofException(size.get() < receivingSize ? UNEXPECTED_END_OF_STREAM : UNEXPECTED_DATA);
													})
													.whenComplete(downloadFinishPromise.recordStats())
													.whenComplete(toLogger(logger, "onDownloadComplete", name, offset, limit, this))
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
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
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
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
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
	public Promise<Map<String, FileMetadata>> list(@NotNull String glob) {
		return simpleCommand(new RemoteFsCommands.List(glob), ListFinished.class, ListFinished::getFiles)
				.whenComplete(toLogger(logger, "list", glob, this))
				.whenComplete(listPromise.recordStats());
	}

	@Override
	public Promise<@Nullable FileMetadata> info(@NotNull String name) {
		return simpleCommand(new Info(name), InfoFinished.class, InfoFinished::getMetadata)
				.whenComplete(toLogger(logger, "info", name, this))
				.whenComplete(infoPromise.recordStats());
	}

	@Override
	public Promise<Map<String, @NotNull FileMetadata>> infoAll(@NotNull Set<String> names) {
		if (names.isEmpty()) return Promise.of(emptyMap());

		return simpleCommand(new InfoAll(names), InfoAllFinished.class, InfoAllFinished::getMetadataMap)
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
		return doConnect(address, socketSettings);
	}

	private Promise<MessagingWithBinaryStreaming<FsResponse, FsCommand>> connectForStreaming(InetSocketAddress address) {
		return doConnect(address, socketSettings.withLingerTimeout(Duration.ZERO));
	}

	private Promise<MessagingWithBinaryStreaming<FsResponse, FsCommand>> doConnect(InetSocketAddress address, SocketSettings socketSettings) {
		return AsyncTcpSocketNio.connect(address, connectionTimeout, socketSettings)
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
			return Promise.ofException(((ServerError) msg).getError());
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
		return "ClusterActiveFs{address=" + address + '}';
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
	public PromiseStats getAppendStartPromise() {
		return appendStartPromise;
	}

	@JmxAttribute
	public PromiseStats getAppendFinishPromise() {
		return appendFinishPromise;
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
