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
import io.activej.csp.ChannelConsumers;
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
import java.util.function.Function;

import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.remotefs.RemoteFsUtils.KNOWN_ERRORS;
import static io.activej.remotefs.RemoteFsUtils.nullTerminatedJson;

/**
 * An implementation of {@link FsClient} which connects to a single {@link RemoteFsServer} and communicates with it.
 */
public final class RemoteFsClient implements FsClient, EventloopService, EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsClient.class);

	public static final StacklessException INVALID_MESSAGE = new StacklessException(RemoteFsClient.class, "Invalid or unexpected message received");
	public static final StacklessException TOO_MUCH_DATA = new StacklessException(RemoteFsClient.class, "Received more bytes than expected");
	public static final StacklessException UNEXPECTED_END_OF_STREAM = new StacklessException(RemoteFsClient.class, "Unexpected end of stream");
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
	private final PromiseStats movePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats copyPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats listPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deletePromise = PromiseStats.create(Duration.ofMinutes(5));
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
								.then(msg -> {
									if (!(msg instanceof UploadAck)) {
										return handleInvalidResponse(msg);
									}
									if (!((UploadAck) msg).isOk()) {
										return Promise.of(ChannelConsumers.<ByteBuf>recycling());
									}
									return Promise.of(messaging.sendBinaryStream()
											.withAcknowledgement(ack -> ack
													.then(messaging::receive)
													.then(msg2 -> {
														messaging.close();
														return msg2 instanceof UploadFinished ?
																Promise.complete() :
																handleInvalidResponse(msg2);
													})
													.whenException(e -> {
														messaging.closeEx(e);
														logger.warn("Cancelled while trying to upload file {} : {}", filename, this, e);
													})
													.whenComplete(uploadFinishPromise.recordStats())));
								})
								.whenException(e -> {
									messaging.closeEx(e);
									logger.warn("Error while trying to upload file {} : {}", filename, this, e);
								}))
				.whenComplete(toLogger(logger, "upload", filename, this))
				.whenComplete(uploadStartPromise.recordStats());
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long length) {
		return connect(address)
				.then(messaging ->
						messaging.send(new Download(name, offset, length))
								.then(messaging::receive)
								.then(msg -> {
									if (!(msg instanceof DownloadSize)) {
										return handleInvalidResponse(msg);
									}
									long receivingSize = ((DownloadSize) msg).getSize();

									logger.trace("download size for file {} is {}: {}", name, receivingSize, this);

									RefLong size = new RefLong(0);
									return Promise.of(messaging.receiveBinaryStream()
											.peek(buf -> size.inc(buf.readRemaining()))
											.withEndOfStream(eos -> eos
													.then(messaging::sendEndOfStream)
													.then(result -> {
														if (size.get() == receivingSize) {
															return Promise.of(result);
														}
														logger.error("invalid stream size for file {}" +
																" (offset {}, length {})," +
																" expected: {} actual: {}",
																name, offset, length, receivingSize, size.get());
														return Promise.ofException(size.get() < receivingSize ? UNEXPECTED_END_OF_STREAM : TOO_MUCH_DATA);
													})
													.whenComplete(downloadFinishPromise.recordStats())
													.whenResult(messaging::close)));
								})
								.whenException(e -> {
									messaging.closeEx(e);
									logger.warn("error trying to download file {} (offset={}, length={}) : {}", name, offset, length, this, e);
								}))
				.whenComplete(toLogger(logger, "download", name, offset, length, this))
				.whenComplete(downloadStartPromise.recordStats());
	}

	@Override
	public Promise<Void> move(@NotNull String name, @NotNull String target) {
		return simpleCommand(new Move(name, target), MoveFinished.class, $ -> (Void) null)
				.whenComplete(toLogger(logger, "move", name, target, this))
				.whenComplete(movePromise.recordStats());
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target) {
		return simpleCommand(new Copy(name, target), CopyFinished.class, $ -> (Void) null)
				.whenComplete(toLogger(logger, "copy", name, target, this))
				.whenComplete(copyPromise.recordStats());
	}

	@Override
	public Promise<Void> delete(@NotNull String name) {
		return simpleCommand(new Delete(name), DeleteFinished.class, $ -> (Void) null)
				.whenComplete(toLogger(logger, "delete", name, this))
				.whenComplete(deletePromise.recordStats());
	}

	@Override
	public Promise<List<FileMetadata>> list(@NotNull String glob) {
		return simpleCommand(new RemoteFsCommands.List(glob), ListFinished.class, ListFinished::getFiles)
				.whenComplete(toLogger(logger, "list", glob, this))
				.whenComplete(listPromise.recordStats());
	}

	private Promise<MessagingWithBinaryStreaming<FsResponse, FsCommand>> connect(InetSocketAddress address) {
		return AsyncTcpSocketNio.connect(address, 0, socketSettings)
				.map(socket -> MessagingWithBinaryStreaming.create(socket, SERIALIZER))
				.whenResult(() -> logger.trace("connected to [{}]: {}", address, this))
				.whenException(e -> logger.warn("failed connecting to [{}] : {}", address, this, e))
				.whenComplete(connectPromise.recordStats());
	}

	private <T> Promise<T> handleInvalidResponse(@Nullable FsResponse msg) {
		if (msg == null) {
			logger.warn("{}: Received unexpected end of stream", this);
			return Promise.ofException(UNEXPECTED_END_OF_STREAM);
		}
		if (msg instanceof ServerError) {
			int code = ((ServerError) msg).getCode();
			Throwable error = code >= 1 && code <= KNOWN_ERRORS.size() ? KNOWN_ERRORS.get(code - 1) : UNKNOWN_SERVER_ERROR;
			return Promise.ofException(error);
		}
		return Promise.ofException(INVALID_MESSAGE);
	}

	private <T, R extends FsResponse> Promise<T> simpleCommand(FsCommand command, Class<R> responseType, Function<R, T> answerExtractor) {
		return connect(address)
				.then(messaging ->
						messaging.send(command)
								.then(messaging::receive)
								.then(msg -> {
									messaging.close();
									if (msg != null && msg.getClass() == responseType) {
										return Promise.of(answerExtractor.apply(responseType.cast(msg)));
									}
									return handleInvalidResponse(msg);
								})
								.whenException(e -> {
									messaging.closeEx(e);
									logger.warn("Error while processing command {} : {}", command, this, e);
								}));
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return Promise.complete();
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
	public PromiseStats getMovePromise() {
		return movePromise;
	}

	@JmxAttribute
	public PromiseStats getCopyPromise() {
		return copyPromise;
	}

	@JmxAttribute
	public PromiseStats getListPromise() {
		return listPromise;
	}

	@JmxAttribute
	public PromiseStats getDeletePromise() {
		return deletePromise;
	}
	//endregion
}
