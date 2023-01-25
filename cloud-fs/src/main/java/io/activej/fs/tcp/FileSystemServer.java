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

import io.activej.common.function.SupplierEx;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.net.IMessaging;
import io.activej.csp.net.Messaging;
import io.activej.fs.IFileSystem;
import io.activej.fs.exception.FileNotFoundException;
import io.activej.fs.exception.FileSystemException;
import io.activej.fs.tcp.messaging.FileSystemRequest;
import io.activej.fs.tcp.messaging.FileSystemResponse;
import io.activej.fs.tcp.messaging.Version;
import io.activej.fs.util.RemoteFileSystemUtils;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.net.AbstractReactiveServer;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import io.activej.reactor.nio.NioReactor;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.fs.util.RemoteFileSystemUtils.castError;
import static io.activej.fs.util.RemoteFileSystemUtils.ofFixedSize;

/**
 * An implementation of {@link AbstractReactiveServer} that works with {@link FileSystem_Remote} client.
 * It exposes some given {@link IFileSystem} via TCP.
 * <p>
 * <b>This server should not be launched as a publicly available server, it is meant for private networks.</b>
 */
public final class FileSystemServer extends AbstractReactiveServer {
	public static final Version VERSION = new Version(1, 0);

	private static final ByteBufsCodec<FileSystemRequest, FileSystemResponse> SERIALIZER = ByteBufsCodec.ofStreamCodecs(
			RemoteFileSystemUtils.FS_REQUEST_CODEC,
			RemoteFileSystemUtils.FS_RESPONSE_CODEC
	);

	private final IFileSystem fileSystem;

	private Function<FileSystemRequest.Handshake, FileSystemResponse.Handshake> handshakeHandler = $ ->
			new FileSystemResponse.Handshake(null);

	// region JMX
	private final PromiseStats handleRequestPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats handshakePromise = PromiseStats.create(Duration.ofMinutes(5));
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

	private FileSystemServer(NioReactor reactor, IFileSystem fileSystem) {
		super(reactor);
		this.fileSystem = fileSystem;
	}

	public static Builder builder(NioReactor reactor, IFileSystem fileSystem) {
		return new FileSystemServer(reactor, fileSystem).new Builder();
	}

	public final class Builder extends AbstractReactiveServer.Builder<Builder, FileSystemServer> {
		private Builder() {}

		public Builder withHandshakeHandler(Function<FileSystemRequest.Handshake, FileSystemResponse.Handshake> handshakeHandler) {
			checkNotBuilt(this);
			FileSystemServer.this.handshakeHandler = handshakeHandler;
			return this;
		}
	}

	public IFileSystem getFileSystem() {
		return fileSystem;
	}

	@Override
	protected void serve(ITcpSocket socket, InetAddress remoteAddress) {
		Messaging<FileSystemRequest, FileSystemResponse> messaging =
				Messaging.create(socket, SERIALIZER);
		messaging.receive()
				.then(request -> {
					if (!(request instanceof FileSystemRequest.Handshake handshake)) {
						return Promise.ofException(new FileSystemException("Handshake expected"));
					}
					return handleHandshake(messaging, handshake);
				})
				.then(messaging::receive)
				.then(msg -> dispatch(messaging, msg))
				.whenComplete(handleRequestPromise.recordStats())
				.whenException(e -> {
					logger.warn("got an error while handling message : {}", this, e);
					messaging.send(new FileSystemResponse.ServerError(castError(e)))
							.then(messaging::sendEndOfStream)
							.whenResult(messaging::close);
				});
	}

	private Promise<Void> dispatch(Messaging<FileSystemRequest, FileSystemResponse> messaging, FileSystemRequest msg) throws Exception {
		if (msg instanceof FileSystemRequest.Upload upload) {
			return handleUpload(messaging, upload);
		}
		if (msg instanceof FileSystemRequest.Append append) {
			return handleAppend(messaging, append);
		}
		if (msg instanceof FileSystemRequest.Download download) {
			return handleDownload(messaging, download);
		}
		if (msg instanceof FileSystemRequest.Copy copy) {
			return handleCopy(messaging, copy);
		}
		if (msg instanceof FileSystemRequest.CopyAll copyAll) {
			return handleCopyAll(messaging, copyAll);
		}
		if (msg instanceof FileSystemRequest.Move move) {
			return handleMove(messaging, move);
		}
		if (msg instanceof FileSystemRequest.MoveAll moveAll) {
			return handleMoveAll(messaging, moveAll);
		}
		if (msg instanceof FileSystemRequest.Delete delete) {
			return handleDelete(messaging, delete);
		}
		if (msg instanceof FileSystemRequest.DeleteAll deleteAll) {
			return handleDeleteAll(messaging, deleteAll);
		}
		if (msg instanceof FileSystemRequest.List list) {
			return handleList(messaging, list);
		}
		if (msg instanceof FileSystemRequest.Info info) {
			return handleInfo(messaging, info);
		}
		if (msg instanceof FileSystemRequest.InfoAll infoAll) {
			return handleInfoAll(messaging, infoAll);
		}
		if (msg instanceof FileSystemRequest.Ping) {
			return handlePing(messaging);
		}
		if (msg instanceof FileSystemRequest.Handshake) {
			return Promise.ofException(new FileSystemException("Handshake was already performed"));
		}
		throw new AssertionError();
	}

	private Promise<Void> handleHandshake(IMessaging<FileSystemRequest, FileSystemResponse> messaging, FileSystemRequest.Handshake
			handshake) {
		return messaging.send(handshakeHandler.apply(handshake))
				.whenComplete(handshakePromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, "handshake", handshake, this));
	}

	private Promise<Void> handleUpload(IMessaging<FileSystemRequest, FileSystemResponse> messaging, FileSystemRequest.Upload upload) {
		String name = upload.name();
		long size = upload.size();
		return (size == -1 ? fileSystem.upload(name) : fileSystem.upload(name, size))
				.map(uploader -> size == -1 ? uploader : uploader.transformWith(ofFixedSize(size)))
				.then(uploader -> messaging.send(new FileSystemResponse.UploadAck())
						.then(() -> messaging.receiveBinaryStream()
								.streamTo(uploader.withAcknowledgement(
										ack -> ack
												.whenComplete(uploadFinishPromise.recordStats())
												.whenComplete(toLogger(logger, TRACE, "onUploadComplete", upload, this)))
								)))
				.then(() -> messaging.send(new FileSystemResponse.UploadFinished()))
				.then(messaging::sendEndOfStream)
				.whenResult(messaging::close)
				.whenComplete(uploadBeginPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, "upload", upload, this));
	}

	private Promise<Void> handleAppend(IMessaging<FileSystemRequest, FileSystemResponse> messaging, FileSystemRequest.Append append) {
		String name = append.name();
		long offset = append.offset();
		return fileSystem.append(name, offset)
				.then(uploader -> messaging.send(new FileSystemResponse.AppendAck())
						.then(() -> messaging.receiveBinaryStream().streamTo(uploader.withAcknowledgement(
								ack -> ack
										.whenComplete(appendFinishPromise.recordStats())
										.whenComplete(toLogger(logger, TRACE, "onAppendComplete", append, this))))))
				.then(() -> messaging.send(new FileSystemResponse.AppendFinished()))
				.then(messaging::sendEndOfStream)
				.whenResult(messaging::close)
				.whenComplete(appendBeginPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, "append", append, this));

	}

	private Promise<Void> handleDownload(IMessaging<FileSystemRequest, FileSystemResponse> messaging, FileSystemRequest.Download download) {
		String name = download.name();
		long offset = download.offset();
		long limit = download.limit();
		return fileSystem.info(name)
				.whenResult(Objects::isNull, $ -> {
					throw new FileNotFoundException();
				})
				.then(meta -> {
					//noinspection ConstantConditions
					long fixedLimit = Math.max(0, Math.min(meta.getSize() - offset, limit));

					return fileSystem.download(name, offset, fixedLimit)
							.then(supplier -> messaging.send(new FileSystemResponse.DownloadSize(fixedLimit))
									.whenException(supplier::closeEx)
									.then(() -> supplier.streamTo(messaging.sendBinaryStream()
											.withAcknowledgement(ack -> ack
													.whenComplete(toLogger(logger, TRACE, "onDownloadComplete", meta, offset, fixedLimit, this))
													.whenComplete(downloadFinishPromise.recordStats())))))
							.whenComplete(toLogger(logger, "download", meta, offset, fixedLimit, this));
				})
				.whenComplete(downloadBeginPromise.recordStats());
	}

	private Promise<Void> handleCopy(IMessaging<FileSystemRequest, FileSystemResponse> messaging, FileSystemRequest.Copy copy) throws
			Exception {
		return simpleHandle(messaging, () -> fileSystem.copy(copy.name(), copy.target()), FileSystemResponse.CopyFinished::new, copyPromise);
	}

	private Promise<Void> handleCopyAll(IMessaging<FileSystemRequest, FileSystemResponse> messaging, FileSystemRequest.CopyAll copyAll) throws
			Exception {
		return simpleHandle(messaging, () -> fileSystem.copyAll(copyAll.sourceToTarget()), FileSystemResponse.CopyAllFinished::new, copyAllPromise);
	}

	private Promise<Void> handleMove(IMessaging<FileSystemRequest, FileSystemResponse> messaging, FileSystemRequest.Move move) throws
			Exception {
		return simpleHandle(messaging, () -> fileSystem.move(move.name(), move.target()), FileSystemResponse.MoveFinished::new, movePromise);
	}

	private Promise<Void> handleMoveAll(IMessaging<FileSystemRequest, FileSystemResponse> messaging, FileSystemRequest.MoveAll moveAll) throws
			Exception {
		return simpleHandle(messaging, () -> fileSystem.moveAll(moveAll.sourceToTarget()), FileSystemResponse.MoveAllFinished::new, moveAllPromise);
	}

	private Promise<Void> handleDelete(IMessaging<FileSystemRequest, FileSystemResponse> messaging, FileSystemRequest.Delete delete) throws
			Exception {
		return simpleHandle(messaging, () -> fileSystem.delete(delete.name()), FileSystemResponse.DeleteFinished::new, deletePromise);
	}

	private Promise<Void> handleDeleteAll(IMessaging<FileSystemRequest, FileSystemResponse> messaging, FileSystemRequest.DeleteAll
			deleteAll) throws Exception {
		return simpleHandle(messaging, () -> fileSystem.deleteAll(deleteAll.toDelete()), FileSystemResponse.DeleteAllFinished::new, deleteAllPromise);
	}

	private Promise<Void> handleList(IMessaging<FileSystemRequest, FileSystemResponse> messaging, FileSystemRequest.List list) throws
			Exception {
		return simpleHandle(messaging, () -> fileSystem.list(list.glob()), FileSystemResponse.ListFinished::new, listPromise);
	}

	private Promise<Void> handleInfo(IMessaging<FileSystemRequest, FileSystemResponse> messaging, FileSystemRequest.Info info) throws
			Exception {
		return simpleHandle(messaging, () -> fileSystem.info(info.name()), FileSystemResponse.InfoFinished::new, infoPromise);
	}

	private Promise<Void> handleInfoAll(IMessaging<FileSystemRequest, FileSystemResponse> messaging, FileSystemRequest.InfoAll infoAll) throws
			Exception {
		return simpleHandle(messaging, () -> fileSystem.infoAll(infoAll.names()), FileSystemResponse.InfoAllFinished::new, infoAllPromise);
	}

	private Promise<Void> handlePing(IMessaging<FileSystemRequest, FileSystemResponse> messaging) throws Exception {
		return simpleHandle(messaging, fileSystem::ping, FileSystemResponse.Pong::new, pingPromise);
	}

	private Promise<Void> simpleHandle(
			IMessaging<FileSystemRequest, FileSystemResponse> messaging,
			SupplierEx<Promise<Void>> action,
			Supplier<FileSystemResponse> response,
			PromiseStats stats
	) throws Exception {
		return simpleHandle(messaging, action, $ -> response.get(), stats);
	}

	private <R> Promise<Void> simpleHandle(
			IMessaging<FileSystemRequest, FileSystemResponse> messaging,
			SupplierEx<Promise<R>> action,
			Function<R, FileSystemResponse> response,
			PromiseStats stats
	) throws Exception {
		return action.get()
				.then(res -> messaging.send(response.apply(res)))
				.then(messaging::sendEndOfStream)
				.whenComplete(stats.recordStats());
	}

	@Override
	public String toString() {
		return "FileSystemServer(" + fileSystem + ')';
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

	@JmxAttribute
	public PromiseStats getHandshakePromise() {
		return handshakePromise;
	}
	// endregion
}
