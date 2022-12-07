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
import io.activej.csp.net.Messaging;
import io.activej.csp.net.MessagingCodec;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.eventloop.Eventloop;
import io.activej.fs.ActiveFs;
import io.activej.fs.exception.FileNotFoundException;
import io.activej.fs.exception.FsException;
import io.activej.fs.tcp.messaging.FsRequest;
import io.activej.fs.tcp.messaging.FsResponse;
import io.activej.fs.tcp.messaging.Version;
import io.activej.fs.util.RemoteFsUtils;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.net.AbstractServer;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.fs.util.RemoteFsUtils.castError;
import static io.activej.fs.util.RemoteFsUtils.ofFixedSize;

/**
 * An implementation of {@link AbstractServer} that works with {@link RemoteActiveFs} client.
 * It exposes some given {@link ActiveFs} via TCP.
 * <p>
 * <b>This server should not be launched as a publicly available server, it is meant for private networks.</b>
 */
public final class ActiveFsServer extends AbstractServer<ActiveFsServer> {
	public static final Version VERSION = new Version(1, 0);

	private static final ByteBufsCodec<FsRequest, FsResponse> SERIALIZER = MessagingCodec.create(
			RemoteFsUtils.FS_REQUEST_CODEC,
			RemoteFsUtils.FS_RESPONSE_CODEC
	);

	private final ActiveFs fs;

	private Function<FsRequest.Handshake, FsResponse.Handshake> handshakeHandler = $ ->
			new FsResponse.Handshake(null);

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

	private ActiveFsServer(Eventloop eventloop, ActiveFs fs) {
		super(eventloop);
		this.fs = fs;
	}

	public static ActiveFsServer create(Eventloop eventloop, ActiveFs fs) {
		return new ActiveFsServer(eventloop, fs);
	}

	public ActiveFsServer withHandshakeHandler(Function<FsRequest.Handshake, FsResponse.Handshake> handshakeHandler) {
		this.handshakeHandler = handshakeHandler;
		return this;
	}

	public ActiveFs getFs() {
		return fs;
	}

	@Override
	protected void serve(AsyncTcpSocket socket, InetAddress remoteAddress) {
		MessagingWithBinaryStreaming<FsRequest, FsResponse> messaging =
				MessagingWithBinaryStreaming.create(socket, SERIALIZER);
		messaging.receive()
				.then(request -> {
					if (!(request instanceof FsRequest.Handshake handshake)) {
						return Promise.ofException(new FsException("Handshake expected"));
					}
					return handleHandshake(messaging, handshake);
				})
				.then(messaging::receive)
				.then(msg -> dispatch(messaging, msg))
				.whenComplete(handleRequestPromise.recordStats())
				.whenException(e -> {
					logger.warn("got an error while handling message : {}", this, e);
					messaging.send(new FsResponse.ServerError(castError(e)))
							.then(messaging::sendEndOfStream)
							.whenResult(messaging::close);
				});
	}

	private Promise<Void> dispatch(MessagingWithBinaryStreaming<FsRequest, FsResponse> messaging, FsRequest msg) throws Exception {
		if (msg instanceof FsRequest.Upload upload) {
			return handleUpload(messaging, upload);
		}
		if (msg instanceof FsRequest.Append append) {
			return handleAppend(messaging, append);
		}
		if (msg instanceof FsRequest.Download download) {
			return handleDownload(messaging, download);
		}
		if (msg instanceof FsRequest.Copy copy) {
			return handleCopy(messaging, copy);
		}
		if (msg instanceof FsRequest.CopyAll copyAll) {
			return handleCopyAll(messaging, copyAll);
		}
		if (msg instanceof FsRequest.Move move) {
			return handleMove(messaging, move);
		}
		if (msg instanceof FsRequest.MoveAll moveAll) {
			return handleMoveAll(messaging, moveAll);
		}
		if (msg instanceof FsRequest.Delete delete) {
			return handleDelete(messaging, delete);
		}
		if (msg instanceof FsRequest.DeleteAll deleteAll) {
			return handleDeleteAll(messaging, deleteAll);
		}
		if (msg instanceof FsRequest.List list) {
			return handleList(messaging, list);
		}
		if (msg instanceof FsRequest.Info info) {
			return handleInfo(messaging, info);
		}
		if (msg instanceof FsRequest.InfoAll infoAll) {
			return handleInfoAll(messaging, infoAll);
		}
		if (msg instanceof FsRequest.Ping) {
			return handlePing(messaging);
		}
		if (msg instanceof FsRequest.Handshake) {
			return Promise.ofException(new FsException("Handshake was already performed"));
		}
		throw new AssertionError();
	}

	private Promise<Void> handleHandshake(Messaging<FsRequest, FsResponse> messaging, FsRequest.Handshake
			handshake) {
		return messaging.send(handshakeHandler.apply(handshake))
				.whenComplete(handshakePromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, "handshake", handshake, this));
	}

	private Promise<Void> handleUpload(Messaging<FsRequest, FsResponse> messaging, FsRequest.Upload upload) {
		String name = upload.name();
		long size = upload.size();
		return (size == -1 ? fs.upload(name) : fs.upload(name, size))
				.map(uploader -> size == -1 ? uploader : uploader.transformWith(ofFixedSize(size)))
				.then(uploader -> messaging.send(new FsResponse.UploadAck())
						.then(() -> messaging.receiveBinaryStream()
								.streamTo(uploader.withAcknowledgement(
										ack -> ack
												.whenComplete(uploadFinishPromise.recordStats())
												.whenComplete(toLogger(logger, TRACE, "onUploadComplete", upload, this)))
								)))
				.then(() -> messaging.send(new FsResponse.UploadFinished()))
				.then(messaging::sendEndOfStream)
				.whenResult(messaging::close)
				.whenComplete(uploadBeginPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, "upload", upload, this));
	}

	private Promise<Void> handleAppend(Messaging<FsRequest, FsResponse> messaging, FsRequest.Append append) {
		String name = append.name();
		long offset = append.offset();
		return fs.append(name, offset)
				.then(uploader -> messaging.send(new FsResponse.AppendAck())
						.then(() -> messaging.receiveBinaryStream().streamTo(uploader.withAcknowledgement(
								ack -> ack
										.whenComplete(appendFinishPromise.recordStats())
										.whenComplete(toLogger(logger, TRACE, "onAppendComplete", append, this))))))
				.then(() -> messaging.send(new FsResponse.AppendFinished()))
				.then(messaging::sendEndOfStream)
				.whenResult(messaging::close)
				.whenComplete(appendBeginPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, "append", append, this));

	}

	private Promise<Void> handleDownload(Messaging<FsRequest, FsResponse> messaging, FsRequest.Download download) {
		String name = download.name();
		long offset = download.offset();
		long limit = download.limit();
		return fs.info(name)
				.whenResult(Objects::isNull, $ -> {
					throw new FileNotFoundException();
				})
				.then(meta -> {
					//noinspection ConstantConditions
					long fixedLimit = Math.max(0, Math.min(meta.getSize() - offset, limit));

					return fs.download(name, offset, fixedLimit)
							.then(supplier -> messaging.send(new FsResponse.DownloadSize(fixedLimit))
									.whenException(supplier::closeEx)
									.then(() -> supplier.streamTo(messaging.sendBinaryStream()
											.withAcknowledgement(ack -> ack
													.whenComplete(toLogger(logger, TRACE, "onDownloadComplete", meta, offset, fixedLimit, this))
													.whenComplete(downloadFinishPromise.recordStats())))))
							.whenComplete(toLogger(logger, "download", meta, offset, fixedLimit, this));
				})
				.whenComplete(downloadBeginPromise.recordStats());
	}

	private Promise<Void> handleCopy(Messaging<FsRequest, FsResponse> messaging, FsRequest.Copy copy) throws
			Exception {
		return simpleHandle(messaging, () -> fs.copy(copy.name(), copy.target()), FsResponse.CopyFinished::new, copyPromise);
	}

	private Promise<Void> handleCopyAll(Messaging<FsRequest, FsResponse> messaging, FsRequest.CopyAll copyAll) throws
			Exception {
		return simpleHandle(messaging, () -> fs.copyAll(copyAll.sourceToTarget()), FsResponse.CopyAllFinished::new, copyAllPromise);
	}

	private Promise<Void> handleMove(Messaging<FsRequest, FsResponse> messaging, FsRequest.Move move) throws
			Exception {
		return simpleHandle(messaging, () -> fs.move(move.name(), move.target()), FsResponse.MoveFinished::new, movePromise);
	}

	private Promise<Void> handleMoveAll(Messaging<FsRequest, FsResponse> messaging, FsRequest.MoveAll moveAll) throws
			Exception {
		return simpleHandle(messaging, () -> fs.moveAll(moveAll.sourceToTarget()), FsResponse.MoveAllFinished::new, moveAllPromise);
	}

	private Promise<Void> handleDelete(Messaging<FsRequest, FsResponse> messaging, FsRequest.Delete delete) throws
			Exception {
		return simpleHandle(messaging, () -> fs.delete(delete.name()), FsResponse.DeleteFinished::new, deletePromise);
	}

	private Promise<Void> handleDeleteAll(Messaging<FsRequest, FsResponse> messaging, FsRequest.DeleteAll
			deleteAll) throws Exception {
		return simpleHandle(messaging, () -> fs.deleteAll(deleteAll.toDelete()), FsResponse.DeleteAllFinished::new, deleteAllPromise);
	}

	private Promise<Void> handleList(Messaging<FsRequest, FsResponse> messaging, FsRequest.List list) throws
			Exception {
		return simpleHandle(messaging, () -> fs.list(list.glob()), FsResponse.ListFinished::new, listPromise);
	}

	private Promise<Void> handleInfo(Messaging<FsRequest, FsResponse> messaging, FsRequest.Info info) throws
			Exception {
		return simpleHandle(messaging, () -> fs.info(info.name()), FsResponse.InfoFinished::new, infoPromise);
	}

	private Promise<Void> handleInfoAll(Messaging<FsRequest, FsResponse> messaging, FsRequest.InfoAll infoAll) throws
			Exception {
		return simpleHandle(messaging, () -> fs.infoAll(infoAll.names()), FsResponse.InfoAllFinished::new, infoAllPromise);
	}

	private Promise<Void> handlePing(Messaging<FsRequest, FsResponse> messaging) throws Exception {
		return simpleHandle(messaging, fs::ping, FsResponse.Pong::new, pingPromise);
	}

	private Promise<Void> simpleHandle(
			Messaging<FsRequest, FsResponse> messaging,
			SupplierEx<Promise<Void>> action,
			Supplier<FsResponse> response,
			PromiseStats stats
	) throws Exception {
		return simpleHandle(messaging, action, $ -> response.get(), stats);
	}

	private <R> Promise<Void> simpleHandle(
			Messaging<FsRequest, FsResponse> messaging,
			SupplierEx<Promise<R>> action,
			Function<R, FsResponse> response,
			PromiseStats stats
	) throws Exception {
		return action.get()
				.then(res -> messaging.send(response.apply(res)))
				.then(messaging::sendEndOfStream)
				.whenComplete(stats.recordStats());
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

	@JmxAttribute
	public PromiseStats getHandshakePromise() {
		return handshakePromise;
	}
	// endregion
}
