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

import io.activej.common.exception.StacklessException;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.RecyclingChannelConsumer;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.net.Messaging;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.eventloop.Eventloop;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.net.AbstractServer;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import io.activej.remotefs.RemoteFsCommands.*;
import io.activej.remotefs.RemoteFsResponses.*;

import java.net.InetAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.remotefs.FsClient.FILE_NOT_FOUND;
import static io.activej.remotefs.RemoteFsUtils.*;

/**
 * An implementation of {@link AbstractServer} for RemoteFs.
 * It exposes some given {@link FsClient} to the Internet in pair with {@link RemoteFsClient}
 */
public final class RemoteFsServer extends AbstractServer<RemoteFsServer> {
	private static final ByteBufsCodec<FsCommand, FsResponse> SERIALIZER =
			nullTerminatedJson(RemoteFsCommands.CODEC, RemoteFsResponses.CODEC);

	public static final StacklessException NO_HANDLER_FOR_MESSAGE = new StacklessException(RemoteFsServer.class, "No handler for received message type");

	private final Map<Class<?>, MessagingHandler<FsCommand>> handlers = new HashMap<>();
	private final FsClient client;

	// region JMX
	private final PromiseStats handleRequestPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats movePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats copyPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats listPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deletePromise = PromiseStats.create(Duration.ofMinutes(5));
	// endregion

	private RemoteFsServer(Eventloop eventloop, FsClient client) {
		super(eventloop);
		this.client = client;
		addHandlers();
	}

	public static RemoteFsServer create(Eventloop eventloop, Executor executor, Path storage) {
		return new RemoteFsServer(eventloop, LocalFsClient.create(eventloop, executor, storage));
	}

	public static RemoteFsServer create(Eventloop eventloop, FsClient client) {
		return new RemoteFsServer(eventloop, client);
	}

	public FsClient getClient() {
		return client;
	}

	@Override
	protected void serve(AsyncTcpSocket socket, InetAddress remoteAddress) {
		MessagingWithBinaryStreaming<FsCommand, FsResponse> messaging =
				MessagingWithBinaryStreaming.create(socket, SERIALIZER);
		messaging.receive()
				.then(msg -> {
					if (msg == null) {
						logger.warn("unexpected end of stream: {}", this);
						messaging.close();
						return Promise.complete();
					}
					MessagingHandler<FsCommand> handler = handlers.get(msg.getClass());
					if (handler == null) {
						logger.warn("received a message with no associated handler, type: {}", msg.getClass());
						return Promise.ofException(NO_HANDLER_FOR_MESSAGE);
					}
					return handler.onMessage(messaging, msg);
				})
				.whenComplete(handleRequestPromise.recordStats())
				.whenException(e -> {
					logger.warn("got an error while handling message : {}", this, e);
					messaging.send(new ServerError(getErrorCode(e)))
							.then(messaging::sendEndOfStream)
							.whenResult(messaging::close);
				});
	}

	private void addHandlers() {
		onMessage(Upload.class, (messaging, msg) -> {
			String name = msg.getName();
			return client.upload(name)
					.then(uploader -> {
						if (uploader instanceof RecyclingChannelConsumer) {
							return messaging.send(new UploadAck(false));
						}
						return messaging.send(new UploadAck(true))
								.then(() -> messaging.receiveBinaryStream().streamTo(uploader));
					})
					.then(() -> messaging.send(new UploadFinished()))
					.then(messaging::sendEndOfStream)
					.whenResult(messaging::close)
					.whenComplete(uploadPromise.recordStats())
					.whenComplete(toLogger(logger, TRACE, "receiving data", msg, this))
					.toVoid();
		});

		onMessage(Download.class, (messaging, msg) -> {
			String name = msg.getName();
			return client.getMetadata(name)
					.then(meta -> {
						if (meta == null) {
							return Promise.ofException(FILE_NOT_FOUND);
						}
						long size = meta.getSize();
						long offset = msg.getOffset();
						long length = msg.getLength();

						checkRange(size, offset, length);

						long fixedLength = length == -1 ? size - offset : length;

						return messaging.send(new DownloadSize(fixedLength))
								.then(() ->
										ChannelSupplier.ofPromise(client.download(name, offset, fixedLength))
												.streamTo(messaging.sendBinaryStream()))
								.whenComplete(toLogger(logger, "sending data", meta, offset, fixedLength, this));
					})
					.whenComplete(downloadPromise.recordStats());
		});
		onMessage(Move.class, simpleHandler(msg -> client.move(msg.getName(), msg.getTarget()), $ -> new MoveFinished(), movePromise));
		onMessage(Copy.class, simpleHandler(msg -> client.copy(msg.getName(), msg.getTarget()), $ -> new CopyFinished(), copyPromise));
		onMessage(Delete.class, simpleHandler(msg -> client.delete(msg.getName()), $ -> new DeleteFinished(), deletePromise));
		onMessage(List.class, simpleHandler(msg -> client.list(msg.getGlob()), ListFinished::new, listPromise));
	}

	private <T extends FsCommand, R> MessagingHandler<T> simpleHandler(Function<T, Promise<R>> action, Function<R, FsResponse> response, PromiseStats stats) {
		return (messaging, msg) -> action.apply(msg)
				.then(res -> messaging.send(response.apply(res)))
				.then(messaging::sendEndOfStream)
				.whenComplete(stats.recordStats());
	}

	@FunctionalInterface
	private interface MessagingHandler<T extends FsCommand> {
		Promise<Void> onMessage(Messaging<FsCommand, FsResponse> messaging, T item);
	}

	@SuppressWarnings("unchecked")
	private <T extends FsCommand> void onMessage(Class<T> type, MessagingHandler<T> handler) {
		handlers.put(type, (MessagingHandler<FsCommand>) handler);
	}

	@Override
	public String toString() {
		return "RemoteFsServer(" + client + ')';
	}

	// region JMX
	@JmxAttribute
	public PromiseStats getUploadPromise() {
		return uploadPromise;
	}

	@JmxAttribute
	public PromiseStats getDownloadPromise() {
		return downloadPromise;
	}

	@JmxAttribute
	public PromiseStats getMovePromise() {
		return movePromise;
	}

	@JmxAttribute
	public PromiseStats getListPromise() {
		return listPromise;
	}

	@JmxAttribute
	public PromiseStats getDeletePromise() {
		return deletePromise;
	}

	@JmxAttribute
	public PromiseStats getHandleRequestPromise() {
		return handleRequestPromise;
	}
	// endregion
}
