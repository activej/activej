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

import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.net.Messaging;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.eventloop.Eventloop;
import io.activej.fs.ActiveFs;
import io.activej.fs.exception.FsIOException;
import io.activej.fs.exception.scalar.FileNotFoundException;
import io.activej.fs.tcp.RemoteFsCommands.*;
import io.activej.fs.tcp.RemoteFsResponses.*;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.net.AbstractServer;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;

import java.net.InetAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.fs.util.RemoteFsUtils.*;

/**
 * An implementation of {@link AbstractServer} that works with {@link RemoteActiveFs} client.
 * It exposes some given {@link ActiveFs} via TCP.
 * <p>
 * <b>This server should not be launched as a publicly available server, it is meant for private networks.</b>
 */
public final class ActiveFsServer extends AbstractServer<ActiveFsServer> {
	private static final ByteBufsCodec<FsCommand, FsResponse> SERIALIZER =
			nullTerminatedJson(RemoteFsCommands.CODEC, RemoteFsResponses.CODEC);

	public static final FsIOException NO_HANDLER_FOR_MESSAGE = new FsIOException(ActiveFsServer.class, "No handler for received message type");

	private final Map<Class<?>, MessagingHandler<FsCommand>> handlers = new HashMap<>();
	private final ActiveFs fs;

	// region JMX
	private final PromiseStats handleRequestPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats appendBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats appendFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
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
	// endregion

	private ActiveFsServer(Eventloop eventloop, ActiveFs fs) {
		super(eventloop);
		this.fs = fs;
		addHandlers();
	}

	public static ActiveFsServer create(Eventloop eventloop, ActiveFs fs) {
		return new ActiveFsServer(eventloop, fs);
	}

	public ActiveFs getFs() {
		return fs;
	}

	@Override
	protected void serve(AsyncTcpSocket socket, InetAddress remoteAddress) {
		MessagingWithBinaryStreaming<FsCommand, FsResponse> messaging =
				MessagingWithBinaryStreaming.create(socket, SERIALIZER);
		messaging.receive()
				.then(msg -> {
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
					messaging.send(new ServerError(castError(e)))
							.then(messaging::sendEndOfStream)
							.whenResult(messaging::close);
				});
	}

	private void addHandlers() {
		onMessage(Upload.class, (messaging, msg) -> {
			String name = msg.getName();
			Long size = msg.getSize();
			return (size == null ? fs.upload(name) : fs.upload(name, size))
					.map(uploader -> size == null ? uploader : uploader.transformWith(ofFixedSize(size)))
					.then(uploader -> messaging.send(new UploadAck())
							.then(() -> messaging.receiveBinaryStream()
									.streamTo(uploader.withAcknowledgement(
											ack -> ack
													.whenComplete(uploadFinishPromise.recordStats())
													.whenComplete(toLogger(logger, TRACE, "onUploadComplete", msg, this)))
									)))
					.then(() -> messaging.send(new UploadFinished()))
					.then(messaging::sendEndOfStream)
					.whenResult(messaging::close)
					.whenComplete(uploadBeginPromise.recordStats())
					.whenComplete(toLogger(logger, TRACE, "upload", msg, this));
		});

		onMessage(Append.class, (messaging, msg) -> {
			String name = msg.getName();
			long offset = msg.getOffset();
			return fs.append(name, offset)
					.then(uploader -> messaging.send(new AppendAck())
							.then(() -> messaging.receiveBinaryStream().streamTo(uploader.withAcknowledgement(
									ack -> ack
											.whenComplete(appendFinishPromise.recordStats())
											.whenComplete(toLogger(logger, TRACE, "onAppendComplete", msg, this))))))
					.then(() -> messaging.send(new AppendFinished()))
					.then(messaging::sendEndOfStream)
					.whenResult(messaging::close)
					.whenComplete(appendBeginPromise.recordStats())
					.whenComplete(toLogger(logger, TRACE, "append", msg, this));
		});

		onMessage(Download.class, (messaging, msg) -> {
			String name = msg.getName();
			long offset = msg.getOffset();
			long limit = msg.getLimit();
			return fs.info(name)
					.then(meta -> {
						if (meta == null) {
							return Promise.ofException(new FileNotFoundException(ActiveFsServer.class));
						}

						long fixedLimit = Math.max(0, Math.min(meta.getSize() - offset, limit));

						return fs.download(name, offset, fixedLimit)
								.then(supplier -> messaging.send(new DownloadSize(fixedLimit))
										.whenException(supplier::closeEx)
										.then(() -> supplier.streamTo(messaging.sendBinaryStream()
												.withAcknowledgement(ack -> ack
														.whenComplete(toLogger(logger, TRACE, "onDownloadComplete", meta, offset, fixedLimit, this))
														.whenComplete(downloadFinishPromise.recordStats())))))
								.whenComplete(toLogger(logger, "download", meta, offset, fixedLimit, this));
					})
					.whenComplete(downloadBeginPromise.recordStats());
		});
		onMessage(Copy.class, simpleHandler(msg -> fs.copy(msg.getName(), msg.getTarget()), $ -> new CopyFinished(), copyPromise));
		onMessage(CopyAll.class, simpleHandler(msg -> fs.copyAll(msg.getSourceToTarget()), $ -> new CopyAllFinished(), copyAllPromise));
		onMessage(Move.class, simpleHandler(msg -> fs.move(msg.getName(), msg.getTarget()), $ -> new MoveFinished(), movePromise));
		onMessage(MoveAll.class, simpleHandler(msg -> fs.moveAll(msg.getSourceToTarget()), $ -> new MoveAllFinished(), moveAllPromise));
		onMessage(Delete.class, simpleHandler(msg -> fs.delete(msg.getName()), $ -> new DeleteFinished(), deletePromise));
		onMessage(DeleteAll.class, simpleHandler(msg -> fs.deleteAll(msg.getFilesToDelete()), $ -> new DeleteAllFinished(), deleteAllPromise));
		onMessage(List.class, simpleHandler(msg -> fs.list(msg.getGlob()), ListFinished::new, listPromise));
		onMessage(Info.class, simpleHandler(msg -> fs.info(msg.getName()), InfoFinished::new, infoPromise));
		onMessage(InfoAll.class, simpleHandler(msg -> fs.infoAll(msg.getNames()), InfoAllFinished::new, infoAllPromise));
		onMessage(Ping.class, simpleHandler(msg -> fs.ping(), $ -> new PingFinished(), pingPromise));
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
		return "ActiveFsServer(" + fs + ')';
	}

	// region JMX
	@JmxAttribute
	public PromiseStats getUploadBeginPromise() {
		return uploadBeginPromise;
	}

	@JmxAttribute
	public PromiseStats getUploadFinishPromise() {
		return uploadFinishPromise;
	}

	@JmxAttribute
	public PromiseStats getAppendBeginPromise() {
		return appendBeginPromise;
	}

	@JmxAttribute
	public PromiseStats getAppendFinishPromise() {
		return appendFinishPromise;
	}

	@JmxAttribute
	public PromiseStats getDownloadBeginPromise() {
		return downloadBeginPromise;
	}

	@JmxAttribute
	public PromiseStats getDownloadFinishPromise() {
		return downloadFinishPromise;
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
	public PromiseStats getDeletePromise() {
		return deletePromise;
	}

	@JmxAttribute
	public PromiseStats getDeleteAllPromise() {
		return deleteAllPromise;
	}

	@JmxAttribute
	public PromiseStats getHandleRequestPromise() {
		return handleRequestPromise;
	}
	// endregion
}
