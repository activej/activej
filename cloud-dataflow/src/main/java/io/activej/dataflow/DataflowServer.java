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

package io.activej.dataflow;

import io.activej.async.process.AsyncCloseable;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.MemSize;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.net.Messaging;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.csp.queue.ChannelQueue;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.dataflow.command.DataflowCommand;
import io.activej.dataflow.command.DataflowCommandDownload;
import io.activej.dataflow.command.DataflowCommandExecute;
import io.activej.dataflow.command.DataflowResponse;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.TaskContext;
import io.activej.dataflow.inject.BinarySerializerModule.BinarySerializerLocator;
import io.activej.dataflow.node.Node;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.inject.ResourceLocator;
import io.activej.net.AbstractServer;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.SettablePromise;
import io.activej.serializer.BinarySerializer;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Server for processing JSON commands.
 */
@SuppressWarnings("rawtypes")
public final class DataflowServer extends AbstractServer<DataflowServer> {
	private final Map<StreamId, ChannelQueue<ByteBuf>> pendingStreams = new HashMap<>();
	private final Map<Class, CommandHandler> handlers = new HashMap<>();

	private final ByteBufsCodec<DataflowCommand, DataflowResponse> codec;
	private final BinarySerializerLocator serializers;
	private final ResourceLocator environment;

	{
		handlers.put(DataflowCommandDownload.class, new DownloadCommandHandler());
		handlers.put(DataflowCommandExecute.class, new ExecuteCommandHandler());
	}

	protected interface CommandHandler<I, O> {
		void onCommand(Messaging<I, O> messaging, I command);
	}

	public DataflowServer(Eventloop eventloop, ByteBufsCodec<DataflowCommand, DataflowResponse> codec, BinarySerializerLocator serializers, ResourceLocator environment) {
		super(eventloop);
		this.codec = codec;
		this.serializers = serializers;
		this.environment = environment;
	}

	private class DownloadCommandHandler implements CommandHandler<DataflowCommandDownload, DataflowResponse> {
		@Override
		public void onCommand(Messaging<DataflowCommandDownload, DataflowResponse> messaging, DataflowCommandDownload command) {
			if (logger.isTraceEnabled()) {
				logger.trace("Processing onDownload: {}, {}", command, messaging);
			}
			StreamId streamId = command.getStreamId();
			ChannelQueue<ByteBuf> forwarder = pendingStreams.remove(streamId);
			if (forwarder != null) {
				logger.info("onDownload: transferring {}, pending downloads: {}", streamId, pendingStreams.size());
			} else {
				forwarder = new ChannelZeroBuffer<>();
				pendingStreams.put(streamId, forwarder);
				logger.info("onDownload: waiting {}, pending downloads: {}", streamId, pendingStreams.size());
				messaging.receive()
						.whenException(() -> {
							ChannelQueue<ByteBuf> removed = pendingStreams.remove(streamId);
							if (removed != null) {
								logger.info("onDownload: removing {}, pending downloads: {}", streamId, pendingStreams.size());
							}
						});
			}
			ChannelConsumer<ByteBuf> consumer = messaging.sendBinaryStream();
			forwarder.getSupplier().streamTo(consumer);
			consumer.withAcknowledgement(ack ->
					ack.whenComplete(($, e) -> {
						if (e != null) {
							logger.warn("Exception occurred while trying to send data");
						}
						messaging.close();
					}));
		}
	}

	private class ExecuteCommandHandler implements CommandHandler<DataflowCommandExecute, DataflowResponse> {
		@Override
		public void onCommand(Messaging<DataflowCommandExecute, DataflowResponse> messaging, DataflowCommandExecute command) {
			TaskContext task = new TaskContext(environment);
			try {
				for (Node node : command.getNodes()) {
					node.createAndBind(task);
				}
			} catch (Exception e) {
				logger.error("Failed to createAndBind task: {}", command, e);
				sendResponse(messaging, e);
				return;
			}

			task.execute()
					.whenComplete(($, throwable) -> {
						if (throwable == null) {
							logger.info("Task executed successfully: {}", command);
						} else {
							logger.error("Failed to execute task: {}", command, throwable);
						}
						sendResponse(messaging, throwable);
					});

			messaging.receive()
					.whenException(() -> {
						if (!task.isExecuted()) {
							logger.error("Client disconnected. Canceling task: {}", command);
							task.cancel();
						}
					});
		}

		private void sendResponse(Messaging<DataflowCommandExecute, DataflowResponse> messaging, @Nullable Throwable throwable) {
			String error = null;
			if (throwable != null) {
				error = throwable.getClass().getSimpleName() + ": " + throwable.getMessage();
			}
			messaging.send(new DataflowResponse(error))
					.whenComplete(messaging::close);
		}
	}

	public <T> StreamConsumer<T> upload(StreamId streamId, Class<T> type) {
		BinarySerializer<T> serializer = serializers.get(type);

		ChannelSerializer<T> streamSerializer = ChannelSerializer.create(serializer)
				.withInitialBufferSize(MemSize.kilobytes(256))
				.withAutoFlushInterval(Duration.ZERO)
				.withExplicitEndOfStream();

		ChannelQueue<ByteBuf> forwarder = pendingStreams.remove(streamId);
		if (forwarder == null) {
			forwarder = new ChannelZeroBuffer<>();
			pendingStreams.put(streamId, forwarder);
			logger.info("onUpload: waiting {}, pending downloads: {}", streamId, pendingStreams.size());
		} else {
			logger.info("onUpload: transferring {}, pending downloads: {}", streamId, pendingStreams.size());
		}
		streamSerializer.getOutput().set(forwarder.getConsumer());
		streamSerializer.getAcknowledgement()
				.whenException(() -> {
					ChannelQueue<ByteBuf> removed = pendingStreams.remove(streamId);
					if (removed != null) {
						logger.info("onUpload: removing {}, pending downloads: {}", streamId, pendingStreams.size());
						removed.close();
					}
				});
		return streamSerializer;
	}

	@Override
	protected void serve(AsyncTcpSocket socket, InetAddress remoteAddress) {
		Messaging<DataflowCommand, DataflowResponse> messaging = MessagingWithBinaryStreaming.create(socket, codec);
		messaging.receive()
				.whenResult(msg -> {
					if (msg != null) {
						doRead(messaging, msg);
					} else {
						logger.warn("unexpected end of stream");
						messaging.close();
					}
				})
				.whenException(e -> {
					logger.error("received error while trying to read", e);
					messaging.close();
				});
	}

	@SuppressWarnings("unchecked")
	private void doRead(Messaging<DataflowCommand, DataflowResponse> messaging, DataflowCommand command) {
		CommandHandler handler = handlers.get(command.getClass());
		if (handler == null) {
			messaging.close();
			logger.error("missing handler for {}", command);
		} else {
			handler.onCommand(messaging, command);
		}
	}

	@Override
	protected void onClose(SettablePromise<Void> cb) {
		List<ChannelQueue<ByteBuf>> pending = new ArrayList<>(pendingStreams.values());
		pendingStreams.clear();
		pending.forEach(AsyncCloseable::close);
		cb.set(null);
	}
}
