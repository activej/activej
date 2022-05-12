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
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.eventloop.Eventloop;
import io.activej.fs.ActiveFs;
import io.activej.fs.FileMetadata;
import io.activej.fs.exception.*;
import io.activej.fs.tcp.FsMessagingProto.FsRequest;
import io.activej.fs.tcp.FsMessagingProto.FsRequest.*;
import io.activej.fs.tcp.FsMessagingProto.FsResponse;
import io.activej.fs.tcp.FsMessagingProto.FsResponse.*;
import io.activej.fs.tcp.FsMessagingProto.FsResponse.Handshake.Ok;
import io.activej.fs.tcp.FsMessagingProto.FsResponse.ServerError.OneOfFsScalarExceptions;
import io.activej.fs.tcp.FsMessagingProto.Version;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.net.AbstractServer;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.fs.tcp.FsMessagingProto.FsResponse.ResponseCase.*;
import static io.activej.fs.tcp.FsMessagingProto.FsResponse.ServerError.FsBatchException.newBuilder;
import static io.activej.fs.util.ProtobufUtils.codec;
import static io.activej.fs.util.RemoteFsUtils.castError;
import static io.activej.fs.util.RemoteFsUtils.ofFixedSize;

/**
 * An implementation of {@link AbstractServer} that works with {@link RemoteActiveFs} client.
 * It exposes some given {@link ActiveFs} via TCP.
 * <p>
 * <b>This server should not be launched as a publicly available server, it is meant for private networks.</b>
 */
public final class ActiveFsServer extends AbstractServer<ActiveFsServer> {
	public static final Version VERSION = Version.newBuilder().setMajor(1).setMinor(0).build();

	private static final ByteBufsCodec<FsRequest, FsResponse> SERIALIZER = codec(FsRequest.parser());

	private final ActiveFs fs;

	private Function<FsRequest.Handshake, FsResponse.Handshake> handshakeHandler = $ -> FsResponse.Handshake.newBuilder()
			.setOk(Ok.newBuilder())
			.build();

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
				.then(handshakeMsg -> {
					if (!handshakeMsg.hasHandshake()) {
						return Promise.ofException(new FsException("Handshake expected"));
					}
					return handleHandshake(messaging, handshakeMsg.getHandshake())
							.then(() -> messaging.receive()
									.then(msg -> switch (msg.getRequestCase()) {
										case UPLOAD -> handleUpload(messaging, msg.getUpload());
										case APPEND -> handleAppend(messaging, msg.getAppend());
										case DOWNLOAD -> handleDownload(messaging, msg.getDownload());
										case COPY -> handleCopy(messaging, msg.getCopy());
										case COPY_ALL -> handleCopyAll(messaging, msg.getCopyAll());
										case MOVE -> handleMove(messaging, msg.getMove());
										case MOVE_ALL -> handleMoveAll(messaging, msg.getMoveAll());
										case DELETE -> handleDelete(messaging, msg.getDelete());
										case DELETE_ALL -> handleDeleteAll(messaging, msg.getDeleteAll());
										case LIST -> handleList(messaging, msg.getList());
										case INFO -> handleInfo(messaging, msg.getInfo());
										case INFO_ALL -> handleInfoAll(messaging, msg.getInfoAll());
										case PING -> handlePing(messaging);
										case HANDSHAKE ->
												Promise.ofException(new FsException("Handshake was already performed"));
										case REQUEST_NOT_SET ->
												Promise.ofException(new FsException("Request was not set"));
									}));
				})
				.whenComplete(handleRequestPromise.recordStats())
				.whenException(e -> {
					logger.warn("got an error while handling message : {}", this, e);
					messaging.send(serverError(e))
							.then(messaging::sendEndOfStream)
							.whenResult(messaging::close);
				});
	}

	private Promise<Void> handleHandshake(Messaging<FsRequest, FsResponse> messaging, FsRequest.Handshake handshake) {
		return messaging.send(FsResponse.newBuilder().setHandshake(handshakeHandler.apply(handshake)).build())
				.whenComplete(handshakePromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, "handshake", handshake, this));
	}

	private Promise<Void> handleUpload(Messaging<FsRequest, FsResponse> messaging, Upload upload) {
		String name = upload.getName();
		long size = upload.getSize();
		return (size == -1 ? fs.upload(name) : fs.upload(name, size))
				.map(uploader -> size == -1 ? uploader : uploader.transformWith(ofFixedSize(size)))
				.then(uploader -> messaging.send(response(UPLOAD_ACK))
						.then(() -> messaging.receiveBinaryStream()
								.streamTo(uploader.withAcknowledgement(
										ack -> ack
												.whenComplete(uploadFinishPromise.recordStats())
												.whenComplete(toLogger(logger, TRACE, "onUploadComplete", upload, this)))
								)))
				.then(() -> messaging.send(response(UPLOAD_FINISHED)))
				.then(messaging::sendEndOfStream)
				.whenResult(messaging::close)
				.whenComplete(uploadBeginPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, "upload", upload, this));
	}

	private Promise<Void> handleAppend(Messaging<FsRequest, FsResponse> messaging, Append append) {
		String name = append.getName();
		long offset = append.getOffset();
		return fs.append(name, offset)
				.then(uploader -> messaging.send(response(APPEND_ACK))
						.then(() -> messaging.receiveBinaryStream().streamTo(uploader.withAcknowledgement(
								ack -> ack
										.whenComplete(appendFinishPromise.recordStats())
										.whenComplete(toLogger(logger, TRACE, "onAppendComplete", append, this))))))
				.then(() -> messaging.send(response(APPEND_FINISHED)))
				.then(messaging::sendEndOfStream)
				.whenResult(messaging::close)
				.whenComplete(appendBeginPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, "append", append, this));

	}

	private Promise<Void> handleDownload(Messaging<FsRequest, FsResponse> messaging, Download download) {
		String name = download.getName();
		long offset = download.getOffset();
		long limit = download.getLimit();
		return fs.info(name)
				.whenResult(Objects::isNull, $ -> {
					throw new FileNotFoundException();
				})
				.then(meta -> {
					//noinspection ConstantConditions
					long fixedLimit = Math.max(0, Math.min(meta.getSize() - offset, limit));

					return fs.download(name, offset, fixedLimit)
							.then(supplier -> messaging.send(downloadSize(fixedLimit))
									.whenException(supplier::closeEx)
									.then(() -> supplier.streamTo(messaging.sendBinaryStream()
											.withAcknowledgement(ack -> ack
													.whenComplete(toLogger(logger, TRACE, "onDownloadComplete", meta, offset, fixedLimit, this))
													.whenComplete(downloadFinishPromise.recordStats())))))
							.whenComplete(toLogger(logger, "download", meta, offset, fixedLimit, this));
				})
				.whenComplete(downloadBeginPromise.recordStats());
	}

	private Promise<Void> handleCopy(Messaging<FsRequest, FsResponse> messaging, Copy copy) throws Exception {
		return simpleHandle(messaging, () -> fs.copy(copy.getName(), copy.getTarget()), $ -> response(COPY_FINISHED), copyPromise);
	}

	private Promise<Void> handleCopyAll(Messaging<FsRequest, FsResponse> messaging, CopyAll copyAll) throws Exception {
		return simpleHandle(messaging, () -> fs.copyAll(copyAll.getSourceToTargetMap()), $ -> response(COPY_ALL_FINISHED), copyAllPromise);
	}

	private Promise<Void> handleMove(Messaging<FsRequest, FsResponse> messaging, Move move) throws Exception {
		return simpleHandle(messaging, () -> fs.move(move.getName(), move.getTarget()), $ -> response(MOVE_FINISHED), movePromise);
	}

	private Promise<Void> handleMoveAll(Messaging<FsRequest, FsResponse> messaging, MoveAll moveAll) throws Exception {
		return simpleHandle(messaging, () -> fs.moveAll(moveAll.getSourceToTargetMap()), $ -> response(MOVE_ALL_FINISHED), moveAllPromise);
	}

	private Promise<Void> handleDelete(Messaging<FsRequest, FsResponse> messaging, Delete delete) throws Exception {
		return simpleHandle(messaging, () -> fs.delete(delete.getName()), $ -> response(DELETE_FINISHED), deletePromise);
	}

	private Promise<Void> handleDeleteAll(Messaging<FsRequest, FsResponse> messaging, DeleteAll deleteAll) throws Exception {
		return simpleHandle(messaging, () -> fs.deleteAll(listToSet(deleteAll.getToDeleteList())), $ -> response(DELETE_ALL_FINISHED), deleteAllPromise);
	}

	private Promise<Void> handleList(Messaging<FsRequest, FsResponse> messaging, List list) throws Exception {
		return simpleHandle(messaging, () -> fs.list(list.getGlob()), ActiveFsServer::listFinished, listPromise);
	}

	private Promise<Void> handleInfo(Messaging<FsRequest, FsResponse> messaging, Info info) throws Exception {
		return simpleHandle(messaging, () -> fs.info(info.getName()), ActiveFsServer::infoFinished, infoPromise);
	}

	private Promise<Void> handleInfoAll(Messaging<FsRequest, FsResponse> messaging, InfoAll infoAll) throws Exception {
		return simpleHandle(messaging, () -> fs.infoAll(listToSet(infoAll.getNamesList())), ActiveFsServer::infoAllFinished, infoAllPromise);
	}

	private Promise<Void> handlePing(Messaging<FsRequest, FsResponse> messaging) throws Exception {
		return simpleHandle(messaging, fs::ping, $ -> response(PONG), pingPromise);
	}

	private <R> Promise<Void> simpleHandle(Messaging<FsRequest, FsResponse> messaging, SupplierEx<Promise<R>> action, Function<R, FsResponse> response, PromiseStats stats) throws Exception {
		return action.get()
				.then(res -> messaging.send(response.apply(res)))
				.then(messaging::sendEndOfStream)
				.whenComplete(stats.recordStats());
	}

	private static FsResponse response(ResponseCase responseCase) {
		FsResponse.Builder builder = FsResponse.newBuilder();
		return switch (responseCase) {
			case UPLOAD_ACK -> builder.setUploadAck(UploadAck.newBuilder()).build();
			case UPLOAD_FINISHED -> builder.setUploadFinished(UploadFinished.newBuilder()).build();
			case APPEND_ACK -> builder.setAppendAck(AppendAck.newBuilder()).build();
			case APPEND_FINISHED -> builder.setAppendFinished(AppendFinished.newBuilder()).build();
			case COPY_FINISHED -> builder.setCopyFinished(CopyFinished.newBuilder()).build();
			case COPY_ALL_FINISHED -> builder.setCopyAllFinished(CopyAllFinished.newBuilder()).build();
			case MOVE_FINISHED -> builder.setMoveFinished(MoveFinished.newBuilder()).build();
			case MOVE_ALL_FINISHED -> builder.setMoveAllFinished(MoveAllFinished.newBuilder()).build();
			case LIST_FINISHED -> builder.setListFinished(ListFinished.newBuilder()).build();
			case INFO_FINISHED -> builder.setInfoFinished(InfoFinished.newBuilder()).build();
			case INFO_ALL_FINISHED -> builder.setInfoAllFinished(InfoAllFinished.newBuilder()).build();
			case DELETE_FINISHED -> builder.setDeleteFinished(DeleteFinished.newBuilder()).build();
			case DELETE_ALL_FINISHED -> builder.setDeleteAllFinished(DeleteAllFinished.newBuilder()).build();
			case PONG -> builder.setPong(Pong.newBuilder()).build();
			default -> throw new AssertionError();
		};
	}

	private static FsResponse downloadSize(long fixedSize) {
		return FsResponse.newBuilder().setDownloadSize(DownloadSize.newBuilder().setSize(fixedSize)).build();
	}

	private static FsResponse listFinished(Map<String, FileMetadata> files) {
		ListFinished.Builder listFinishedBuilder = ListFinished.newBuilder();
		files.forEach((fileName, fileMetadata) -> listFinishedBuilder.putFiles(fileName, convertFileMetadata(fileMetadata)));
		return FsResponse.newBuilder().setListFinished(listFinishedBuilder).build();
	}

	private static FsResponse infoFinished(@Nullable FileMetadata fileMetadata) {
		InfoFinished.Builder infoFinishedBuilder = InfoFinished.newBuilder();
		NullableFileMetadata.Builder nullableFileMetadataBuilder = NullableFileMetadata.newBuilder();
		if (fileMetadata == null) {
			nullableFileMetadataBuilder.setIsNull(true);
		} else {
			nullableFileMetadataBuilder.setValue(convertFileMetadata(fileMetadata));
		}
		infoFinishedBuilder.setNullableFileMetadata(nullableFileMetadataBuilder);
		return FsResponse.newBuilder().setInfoFinished(infoFinishedBuilder).build();
	}

	private static FsResponse infoAllFinished(Map<String, FileMetadata> files) {
		InfoAllFinished.Builder infoAllFinishedBuilder = InfoAllFinished.newBuilder();
		files.forEach((fileName, fileMetadata) -> infoAllFinishedBuilder.putFiles(fileName, convertFileMetadata(fileMetadata)));
		return FsResponse.newBuilder().setInfoAllFinished(infoAllFinishedBuilder).build();
	}

	private static FsResponse serverError(Exception exception) {
		ServerError.Builder serverErrorBuilder = ServerError.newBuilder();

		FsException fsException = castError(exception);
		Class<? extends FsException> exceptionClass = fsException.getClass();

		if (exceptionClass == FsStateException.class) {
			serverErrorBuilder.setFsStateException(ServerError.FsStateException.newBuilder().setMessage(fsException.getMessage()));
		} else if (exceptionClass == FsScalarException.class) {
			serverErrorBuilder.setFsScalarException(ServerError.FsScalarException.newBuilder().setMessage(fsException.getMessage()));
		} else if (exceptionClass == PathContainsFileException.class) {
			serverErrorBuilder.setPathContainsFileException(ServerError.PathContainsFileException.newBuilder().setMessage(fsException.getMessage()));
		} else if (exceptionClass == IllegalOffsetException.class) {
			serverErrorBuilder.setIllegalOffsetException(ServerError.IllegalOffsetException.newBuilder().setMessage(fsException.getMessage()));
		} else if (exceptionClass == FileNotFoundException.class) {
			serverErrorBuilder.setFileNotFoundException(ServerError.FileNotFoundException.newBuilder().setMessage(fsException.getMessage()));
		} else if (exceptionClass == ForbiddenPathException.class) {
			serverErrorBuilder.setForbiddenPathException(ServerError.ForbiddenPathException.newBuilder().setMessage(fsException.getMessage()));
		} else if (exceptionClass == MalformedGlobException.class) {
			serverErrorBuilder.setMalformedGlobException(ServerError.MalformedGlobException.newBuilder().setMessage(fsException.getMessage()));
		} else if (exceptionClass == IsADirectoryException.class) {
			serverErrorBuilder.setIsADirectoryException(ServerError.IsADirectoryException.newBuilder().setMessage(fsException.getMessage()).build());
		} else if (exceptionClass == FsIOException.class) {
			serverErrorBuilder.setFsIoException(ServerError.FsIOException.newBuilder().setMessage(fsException.getMessage()).build());
		} else if (exceptionClass == FsBatchException.class) {
			ServerError.FsBatchException.Builder batchExceptionBuilder = newBuilder();
			Map<String, FsScalarException> exceptions = ((FsBatchException) fsException).getExceptions();
			exceptions.forEach((fileName, scalarException) -> {
				Class<? extends FsScalarException> scalarExceptionClass = scalarException.getClass();
				OneOfFsScalarExceptions.Builder oneOfFsScalarExceptionsBuilder = OneOfFsScalarExceptions.newBuilder();
				if (scalarExceptionClass == PathContainsFileException.class) {
					oneOfFsScalarExceptionsBuilder.setPathContainsFileException(ServerError.PathContainsFileException.newBuilder().setMessage(scalarException.getMessage()).build());
				} else if (scalarExceptionClass == IllegalOffsetException.class) {
					oneOfFsScalarExceptionsBuilder.setIllegalOffsetException(ServerError.IllegalOffsetException.newBuilder().setMessage(scalarException.getMessage()).build());
				} else if (scalarExceptionClass == FileNotFoundException.class) {
					oneOfFsScalarExceptionsBuilder.setFileNotFoundException(ServerError.FileNotFoundException.newBuilder().setMessage(scalarException.getMessage()).build());
				} else if (scalarExceptionClass == ForbiddenPathException.class) {
					oneOfFsScalarExceptionsBuilder.setForbiddenPathException(ServerError.ForbiddenPathException.newBuilder().setMessage(scalarException.getMessage()).build());
				} else if (scalarExceptionClass == MalformedGlobException.class) {
					oneOfFsScalarExceptionsBuilder.setMalformedGlobException(ServerError.MalformedGlobException.newBuilder().setMessage(scalarException.getMessage()).build());
				} else if (scalarExceptionClass == IsADirectoryException.class) {
					oneOfFsScalarExceptionsBuilder.setIsADirectoryException(ServerError.IsADirectoryException.newBuilder().setMessage(scalarException.getMessage()).build());
				} else {
					oneOfFsScalarExceptionsBuilder.setFsScalarException(ServerError.FsScalarException.newBuilder().setMessage(scalarException.getMessage()).build());
				}
				batchExceptionBuilder.putExceptions(fileName, oneOfFsScalarExceptionsBuilder.build());
			});
			serverErrorBuilder.setFsBatchException(batchExceptionBuilder);
		} else {
			serverErrorBuilder.setFsException(ServerError.FsException.newBuilder().setMessage(fsException.getMessage()));
		}
		return FsResponse.newBuilder().setServerError(serverErrorBuilder).build();
	}

	private static FsResponse.FileMetadata convertFileMetadata(FileMetadata fileMetadata) {
		return FsResponse.FileMetadata.newBuilder().setSize(fileMetadata.getSize()).setTimestamp(fileMetadata.getTimestamp()).build();
	}

	private static <T> Set<T> listToSet(java.util.List<T> list) throws FsException {
		Set<T> set = new HashSet<>(list);
		if (set.size() != list.size()) {
			throw new FsException("Found duplicate values in a set: " + list);
		}
		return set;
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
