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

import io.activej.async.service.ReactiveService;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.ApplicationSettings;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.exception.TruncatedDataException;
import io.activej.common.exception.UnexpectedDataException;
import io.activej.common.function.ConsumerEx;
import io.activej.common.function.FunctionEx;
import io.activej.common.ref.RefLong;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.net.IMessaging;
import io.activej.csp.net.Messaging;
import io.activej.fs.FileMetadata;
import io.activej.fs.IFileSystem;
import io.activej.fs.exception.FileSystemException;
import io.activej.fs.tcp.messaging.FileSystemRequest;
import io.activej.fs.tcp.messaging.FileSystemResponse;
import io.activej.fs.util.RemoteFileSystemUtils;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.net.socket.tcp.TcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import io.activej.reactor.AbstractNioReactive;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;
import io.activej.reactor.net.SocketSettings;
import io.activej.reactor.nio.NioReactor;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.isBijection;
import static io.activej.csp.dsl.ChannelConsumerTransformer.identity;
import static io.activej.fs.util.RemoteFileSystemUtils.ofFixedSize;
import static io.activej.reactor.Reactive.checkInReactorThread;

/**
 * A client to the remote {@link FileSystemServer}.
 * All client/server communication is done via TCP.
 * <p>
 * <b>This client should only be used on private networks.</b>
 * <p>
 * Inherits all the limitations of {@link IFileSystem} implementation located on {@link FileSystemServer}.
 */
public final class RemoteFileSystem extends AbstractNioReactive
		implements IFileSystem, ReactiveService, ReactiveJmxBeanWithStats {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFileSystem.class);

	public static final Duration DEFAULT_CONNECTION_TIMEOUT = ApplicationSettings.getDuration(RemoteFileSystem.class, "connectTimeout", Duration.ZERO);

	private static final ByteBufsCodec<FileSystemResponse, FileSystemRequest> SERIALIZER = ByteBufsCodec.ofStreamCodecs(
			RemoteFileSystemUtils.FS_RESPONSE_CODEC,
			RemoteFileSystemUtils.FS_REQUEST_CODEC
	);

	private final InetSocketAddress address;

	private SocketSettings socketSettings = SocketSettings.createDefault();
	private SocketSettings socketSettingsStreaming = createSocketSettingsForStreaming(socketSettings);
	private int connectionTimeout = (int) DEFAULT_CONNECTION_TIMEOUT.toMillis();

	//region JMX
	private final PromiseStats connectPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats handshakePromise = PromiseStats.create(Duration.ofMinutes(5));
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

	private RemoteFileSystem(NioReactor reactor, InetSocketAddress address) {
		super(reactor);
		this.address = address;
	}

	public static RemoteFileSystem create(NioReactor reactor, InetSocketAddress address) {
		return builder(reactor, address).build();
	}

	public static Builder builder(NioReactor reactor, InetSocketAddress address) {
		return new RemoteFileSystem(reactor, address).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, RemoteFileSystem> {
		private Builder() {}

		public Builder withSocketSettings(SocketSettings socketSettings) {
			checkNotBuilt(this);
			RemoteFileSystem.this.socketSettings = socketSettings;
			RemoteFileSystem.this.socketSettingsStreaming = createSocketSettingsForStreaming(socketSettings);
			return this;
		}

		public Builder withConnectionTimeout(Duration connectionTimeout) {
			checkNotBuilt(this);
			RemoteFileSystem.this.connectionTimeout = (int) connectionTimeout.toMillis();
			return this;
		}

		@Override
		protected RemoteFileSystem doBuild() {
			return RemoteFileSystem.this;
		}
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name) {
		checkInReactorThread(this);
		return connectForStreaming(address)
				.then(this::performHandshake)
				.then(messaging -> doUpload(messaging, name, null))
				.whenComplete(uploadStartPromise.recordStats())
				.whenComplete(toLogger(logger, "upload", name, this));
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name, long size) {
		checkInReactorThread(this);
		return connect(address)
				.then(this::performHandshake)
				.then(messaging -> doUpload(messaging, name, size))
				.whenComplete(uploadStartPromise.recordStats())
				.whenComplete(toLogger(logger, "upload", name, size, this));
	}

	private Promise<ChannelConsumer<ByteBuf>> doUpload(IMessaging<FileSystemResponse, FileSystemRequest> messaging, String name, @Nullable Long size) {
		return messaging.send(new FileSystemRequest.Upload(name, size == null ? -1 : size))
				.then(messaging::receive)
				.whenResult(validateFn(FileSystemResponse.UploadAck.class))
				.then(() -> Promise.of(messaging.sendBinaryStream()
						.transformWith(size == null ? identity() : ofFixedSize(size))
						.withAcknowledgement(ack -> ack
								.then(messaging::receive)
								.whenResult(messaging::close)
								.whenResult(validateFn(FileSystemResponse.UploadFinished.class))
								.toVoid()
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
	public Promise<ChannelConsumer<ByteBuf>> append(String name, long offset) {
		checkInReactorThread(this);
		return connect(address)
				.then(this::performHandshake)
				.then(messaging ->
						messaging.send(new FileSystemRequest.Append(name, offset))
								.then(messaging::receive)
								.whenResult(validateFn(FileSystemResponse.AppendAck.class))
								.then(() -> Promise.of(messaging.sendBinaryStream()
										.withAcknowledgement(ack -> ack
												.then(messaging::receive)
												.whenResult(messaging::close)
												.whenResult(validateFn(FileSystemResponse.AppendFinished.class))
												.toVoid()
												.whenException(messaging::closeEx)
												.whenComplete(appendFinishPromise.recordStats())
												.whenComplete(toLogger(logger, TRACE, "onAppendComplete", name, offset, this)))))
								.whenException(messaging::closeEx))
				.whenComplete(appendStartPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, "append", name, offset, this));
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String name, long offset, long limit) {
		checkInReactorThread(this);
		checkArgument(offset >= 0, "Data offset must be greater than or equal to zero");
		checkArgument(limit >= 0, "Data limit must be greater than or equal to zero");

		return connect(address)
				.then(this::performHandshake)
				.then(messaging ->
						messaging.send(new FileSystemRequest.Download(name, offset, limit))
								.then(messaging::receive)
								.map(castFn(FileSystemResponse.DownloadSize.class))
								.then(msg -> {
									long receivingSize = msg.size();
									if (receivingSize > limit) {
										throw new UnexpectedDataException();
									}

									logger.trace("download size for file {} is {}: {}", name, receivingSize, this);

									RefLong size = new RefLong(0);
									return Promise.of(messaging.receiveBinaryStream()
											.peek(buf -> size.inc(buf.readRemaining()))
											.withEndOfStream(eos -> eos
													.then(messaging::sendEndOfStream)
													.whenResult(() -> {
														if (size.get() == receivingSize) {
															return;
														}
														logger.error("invalid stream size for file {} (offset {}, limit {}), expected: {} actual: {}",
																name, offset, limit, receivingSize, size.get());
														throw size.get() < receivingSize ?
																new TruncatedDataException() :
																new UnexpectedDataException();
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
	public Promise<Void> copy(String name, String target) {
		checkInReactorThread(this);
		return simpleCommand(new FileSystemRequest.Copy(name, target), FileSystemResponse.CopyFinished.class)
				.whenComplete(toLogger(logger, "copy", name, target, this))
				.whenComplete(copyPromise.recordStats());
	}

	@Override
	public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
		checkInReactorThread(this);
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return simpleCommand(new FileSystemRequest.CopyAll(sourceToTarget), FileSystemResponse.CopyAllFinished.class)
				.whenComplete(toLogger(logger, "copyAll", sourceToTarget, this))
				.whenComplete(copyAllPromise.recordStats());
	}

	@Override
	public Promise<Void> move(String name, String target) {
		checkInReactorThread(this);
		return simpleCommand(new FileSystemRequest.Move(name, target), FileSystemResponse.MoveFinished.class)
				.whenComplete(toLogger(logger, "move", name, target, this))
				.whenComplete(movePromise.recordStats());
	}

	@Override
	public Promise<Void> moveAll(Map<String, String> sourceToTarget) {
		checkInReactorThread(this);
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return simpleCommand(new FileSystemRequest.MoveAll(sourceToTarget), FileSystemResponse.MoveAllFinished.class)
				.whenComplete(toLogger(logger, "moveAll", sourceToTarget, this))
				.whenComplete(moveAllPromise.recordStats());
	}

	@Override
	public Promise<Void> delete(String name) {
		checkInReactorThread(this);
		return simpleCommand(new FileSystemRequest.Delete(name), FileSystemResponse.DeleteFinished.class)
				.whenComplete(toLogger(logger, "delete", name, this))
				.whenComplete(deletePromise.recordStats());
	}

	@Override
	public Promise<Void> deleteAll(Set<String> toDelete) {
		checkInReactorThread(this);
		if (toDelete.isEmpty()) return Promise.complete();

		return simpleCommand(new FileSystemRequest.DeleteAll(toDelete), FileSystemResponse.DeleteAllFinished.class)
				.whenComplete(toLogger(logger, "deleteAll", toDelete, this))
				.whenComplete(deleteAllPromise.recordStats());
	}

	@Override
	public Promise<Map<String, FileMetadata>> list(String glob) {
		checkInReactorThread(this);
		return simpleCommand(new FileSystemRequest.List(glob), FileSystemResponse.ListFinished.class, FileSystemResponse.ListFinished::files)
				.whenComplete(toLogger(logger, "list", glob, this))
				.whenComplete(listPromise.recordStats());
	}

	@Override
	public Promise<@Nullable FileMetadata> info(String name) {
		checkInReactorThread(this);
		return simpleCommand(new FileSystemRequest.Info(name), FileSystemResponse.InfoFinished.class, FileSystemResponse.InfoFinished::fileMetadata)
				.whenComplete(toLogger(logger, "info", name, this))
				.whenComplete(infoPromise.recordStats());
	}

	@Override
	public Promise<Map<String, FileMetadata>> infoAll(Set<String> names) {
		checkInReactorThread(this);
		if (names.isEmpty()) return Promise.of(Map.of());

		return simpleCommand(new FileSystemRequest.InfoAll(names), FileSystemResponse.InfoAllFinished.class, FileSystemResponse.InfoAllFinished::files)
				.whenComplete(toLogger(logger, "infoAll", names, this))
				.whenComplete(infoAllPromise.recordStats());
	}

	@Override
	public Promise<Void> ping() {
		checkInReactorThread(this);
		return simpleCommand(new FileSystemRequest.Ping(), FileSystemResponse.Pong.class)
				.whenComplete(toLogger(logger, "ping", this))
				.whenComplete(pingPromise.recordStats());
	}

	private Promise<Messaging<FileSystemResponse, FileSystemRequest>> connect(InetSocketAddress address) {
		return doConnect(address, socketSettings);
	}

	private Promise<Messaging<FileSystemResponse, FileSystemRequest>> connectForStreaming(InetSocketAddress address) {
		return doConnect(address, socketSettingsStreaming);
	}

	private Promise<Messaging<FileSystemResponse, FileSystemRequest>> doConnect(InetSocketAddress address, SocketSettings socketSettings) {
		return TcpSocket.connect(reactor, address, connectionTimeout, socketSettings)
				.map(socket -> Messaging.create(socket, SERIALIZER))
				.whenResult(() -> logger.trace("connected to [{}]: {}", address, this))
				.whenException(e -> logger.warn("failed connecting to [{}] : {}", address, this, e))
				.whenComplete(connectPromise.recordStats());
	}

	private Promise<IMessaging<FileSystemResponse, FileSystemRequest>> performHandshake(IMessaging<FileSystemResponse, FileSystemRequest> messaging) {
		return messaging.send(new FileSystemRequest.Handshake(FileSystemServer.VERSION))
				.then(messaging::receive)
				.map(castFn(FileSystemResponse.Handshake.class))
				.map(handshakeResponse -> {
					FileSystemResponse.HandshakeFailure handshakeFailure = handshakeResponse.handshakeFailure();
					if (handshakeFailure != null) {
						throw new FileSystemException(String.format("Handshake failed: %s. Minimal allowed version: %s",
								handshakeFailure.message(), handshakeFailure.minimalVersion()));
					}
					return messaging;
				})
				.whenComplete(toLogger(logger, "handshake", this))
				.whenComplete(handshakePromise.recordStats());
	}

	private static <T extends FileSystemResponse> ConsumerEx<FileSystemResponse> validateFn(Class<T> responseClass) {
		return fileSystemResponse -> castFn(responseClass).apply(fileSystemResponse);
	}

	private static <T extends FileSystemResponse> FunctionEx<FileSystemResponse, T> castFn(Class<T> responseClass) {
		return msg -> {
			if (msg instanceof FileSystemResponse.ServerError serverError) {
				throw serverError.exception();
			}
			if (msg.getClass() != responseClass) {
				throw new FileSystemException("Received request " + msg.getClass().getName() + " instead of " + responseClass);
			}
			//noinspection unchecked
			return ((T) msg);
		};
	}

	private <T extends FileSystemResponse> Promise<Void> simpleCommand(FileSystemRequest command, Class<T> responseCls) {
		return simpleCommand(command, responseCls, $ -> null);
	}

	private <T extends FileSystemResponse, U> Promise<U> simpleCommand(FileSystemRequest command, Class<T> responseCls, FunctionEx<T, U> answerExtractor) {
		return connect(address)
				.then(this::performHandshake)
				.then(messaging ->
						messaging.send(command)
								.then(messaging::receive)
								.whenResult(messaging::close)
								.map(castFn(responseCls))
								.map(answerExtractor)
								.whenException(e -> {
									messaging.closeEx(e);
									logger.warn("Error while processing command {} : {}", command, this, e);
								}));
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

	private static SocketSettings createSocketSettingsForStreaming(SocketSettings socketSettings) {
		SocketSettings.Builder builder = SocketSettings.builder()
				.withLingerTimeout(Duration.ZERO);

		if (socketSettings.hasKeepAlive()) builder.withKeepAlive(socketSettings.getKeepAlive());
		if (socketSettings.hasReuseAddress()) builder.withReuseAddress(socketSettings.getReuseAddress());
		if (socketSettings.hasTcpNoDelay()) builder.withTcpNoDelay(socketSettings.getTcpNoDelay());
		if (socketSettings.hasSendBufferSize()) builder.withSendBufferSize(socketSettings.getSendBufferSize());
		if (socketSettings.hasReceiveBufferSize()) builder.withReceiveBufferSize(socketSettings.getReceiveBufferSize());
		if (socketSettings.hasImplReadTimeout()) builder.withImplReadTimeout(socketSettings.getImplReadTimeout());
		if (socketSettings.hasImplWriteTimeout()) builder.withImplWriteTimeout(socketSettings.getImplWriteTimeout());
		if (socketSettings.hasReadBufferSize()) builder.withImplReadBufferSize(socketSettings.getImplReadBufferSize());

		return builder.build();
	}

	@Override
	public String toString() {
		return "FileSystem_Remote{address=" + address + '}';
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

	@JmxAttribute
	public PromiseStats getHandshakePromise() {
		return handshakePromise;
	}
	//endregion
}
