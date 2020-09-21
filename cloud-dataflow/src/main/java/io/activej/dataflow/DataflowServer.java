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
import io.activej.common.ApplicationSettings;
import io.activej.common.MemSize;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.dsl.ChannelTransformer;
import io.activej.csp.net.Messaging;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.csp.queue.ChannelQueue;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.dataflow.command.*;
import io.activej.dataflow.command.DataflowResponsePartitionData.TaskDesc;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.Task;
import io.activej.dataflow.inject.BinarySerializerModule.BinarySerializerLocator;
import io.activej.dataflow.node.Node;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.inject.ResourceLocator;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.net.AbstractServer;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.Nullable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.time.Duration;
import java.util.*;
import java.util.function.BiConsumer;

import static io.activej.async.process.AsyncCloseable.CLOSE_EXCEPTION;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * Server for processing JSON commands.
 */
@SuppressWarnings("rawtypes")
public final class DataflowServer extends AbstractServer<DataflowServer> {
	private static final int MAX_LAST_RAN_TASKS = ApplicationSettings.getInt(DataflowServer.class, "maxLastRanTasks", 1000);

	private final Map<StreamId, ChannelQueue<ByteBuf>> pendingStreams = new HashMap<>();
	private final Map<Class, BiConsumer<Messaging<DataflowCommand, DataflowResponse>, ?>> handlers = new HashMap<>();

	private final ByteBufsCodec<DataflowCommand, DataflowResponse> codec;
	private final BinarySerializerLocator serializers;

	private final Map<Long, Task> runningTasks = new HashMap<>();
	private final Map<Long, Task> lastTasks = new LinkedHashMap<Long, Task>() {
		@Override
		protected boolean removeEldestEntry(Map.Entry eldest) {
			return size() > MAX_LAST_RAN_TASKS;
		}
	};

	private int succeededTasks = 0, canceledTasks = 0, failedTasks = 0;

	private <T> void handleCommand(Class<T> cls, BiConsumer<Messaging<DataflowCommand, DataflowResponse>, T> handler) {
		handlers.put(cls, handler);
	}

	public DataflowServer(Eventloop eventloop, ByteBufsCodec<DataflowCommand, DataflowResponse> codec, BinarySerializerLocator serializers, ResourceLocator environment) {
		super(eventloop);
		this.codec = codec;
		this.serializers = serializers;

		handleCommand(DataflowCommandDownload.class, (messaging, command) -> {
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
							logger.warn("Exception occurred while trying to send data", e);
						}
						messaging.close();
					}));
		});
		handleCommand(DataflowCommandExecute.class, (messaging, command) -> {
			long taskId = command.getTaskId();
			Task task = new Task(taskId, environment, command.getNodes());
			try {
				task.bind();
			} catch (Exception e) {
				logger.error("Failed to construct task: {}", command, e);
				sendResponse(messaging, e);
				return;
			}
			lastTasks.put(taskId, task);
			runningTasks.put(taskId, task);
			task.execute()
					.whenComplete(($, throwable) -> {
						runningTasks.remove(taskId);
						if (throwable == null) {
							succeededTasks++;
							logger.info("Task executed successfully: {}", command);
						} else if (throwable == CLOSE_EXCEPTION) {
							canceledTasks++;
							logger.error("Canceled task: {}", command, throwable);
						} else {
							failedTasks++;
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
		});
		handleCommand(DataflowCommandGetTasks.class, (messaging, command) -> {
			Long taskId = command.getTaskId();
			if (taskId == null) {
				messaging.send(new DataflowResponsePartitionData(
						runningTasks.size(), succeededTasks, failedTasks, canceledTasks,
						lastTasks.entrySet().stream()
								.map(e -> new TaskDesc(e.getKey(), e.getValue().getStatus()))
								.collect(toList())
				)).whenException(e -> logger.error("Failed to send answer for the partition data request", e));
				return;
			}
			Task task = lastTasks.get(taskId);
			if (task == null) {
				messaging.send(new DataflowResponseResult("No task found with id " + taskId));
				return;
			}
			String err;
			if (task.getError() != null) {
				StringWriter writer = new StringWriter();
				task.getError().printStackTrace(new PrintWriter(writer));
				err = writer.toString();
			} else {
				err = null;
			}
			messaging.send(new DataflowResponseTaskData(
					task.getStatus(),
					task.getStartTime(),
					task.getFinishTime(),
					err,
					task.getNodes().stream()
							.filter(n -> n.getStats() != null)
							.collect(toMap(Node::getIndex, Node::getStats)), task.getGraphViz()))
					.whenException(e -> logger.error("Failed to send answer for the task (" + taskId + ") data request", e));
		});
	}

	private void sendResponse(Messaging<DataflowCommand, DataflowResponse> messaging, @Nullable Throwable throwable) {
		String error = null;
		if (throwable != null) {
			error = throwable.getClass().getSimpleName() + ": " + throwable.getMessage();
		}
		messaging.send(new DataflowResponseResult(error))
				.whenComplete(messaging::close);
	}

	public <T> StreamConsumer<T> upload(StreamId streamId, Class<T> type, ChannelTransformer<ByteBuf, ByteBuf> transformer) {
		ChannelSerializer<T> streamSerializer = ChannelSerializer.create(serializers.get(type))
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

		streamSerializer.getOutput().set(forwarder.getConsumer().transformWith(transformer));
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

	public <T> StreamConsumer<T> upload(StreamId streamId, Class<T> type) {
		return upload(streamId, type, ChannelTransformer.identity());
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
		BiConsumer<Messaging<DataflowCommand, DataflowResponse>, DataflowCommand> handler =
				(BiConsumer<Messaging<DataflowCommand, DataflowResponse>, DataflowCommand>) handlers.get(command.getClass());
		if (handler != null) {
			handler.accept(messaging, command);
			return;
		}
		logger.error("missing handler for {}", command);
		messaging.close();
	}

	@Override
	protected void onClose(SettablePromise<Void> cb) {
		List<ChannelQueue<ByteBuf>> pending = new ArrayList<>(pendingStreams.values());
		pendingStreams.clear();
		pending.forEach(AsyncCloseable::close);
		cb.set(null);
	}

	public Map<Long, Task> getLastTasks() {
		return lastTasks;
	}

	@JmxAttribute
	public int getRunningTasks() {
		return runningTasks.size();
	}

	@JmxAttribute
	public int getSucceededTasks() {
		return succeededTasks;
	}

	@JmxAttribute
	public int getFailedTasks() {
		return failedTasks;
	}

	@JmxAttribute
	public int getCanceledTasks() {
		return canceledTasks;
	}

	@JmxOperation
	public void cancelAll() {
		runningTasks.values().forEach(Task::cancel);
	}

	@JmxOperation
	public boolean cancel(long taskID) {
		Task task = runningTasks.get(taskID);
		if (task != null) {
			task.cancel();
			return true;
		}
		return false;
	}

	@JmxOperation
	public void cancelTask(long id) {
		Task task = runningTasks.get(id);
		if (task != null) {
			task.cancel();
		}
	}
}
