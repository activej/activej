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

import io.activej.async.exception.AsyncCloseException;
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
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.Task;
import io.activej.dataflow.inject.BinarySerializerModule.BinarySerializerLocator;
import io.activej.dataflow.node.Node;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowRequest;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowRequest.Download;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowRequest.Execute;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowRequest.GetTasks;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowRequest.GetTasks.TaskId;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowResponse;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowResponse.Handshake.Ok;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowResponse.PartitionData.TaskDesc;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowResponse.TaskData;
import io.activej.dataflow.proto.DataflowMessagingProto.Version;
import io.activej.dataflow.proto.FunctionSerializer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.inject.ResourceLocator;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.net.AbstractServer;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.Nullable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;

import static io.activej.dataflow.proto.ProtobufUtils.convert;
import static io.activej.dataflow.proto.ProtobufUtils.error;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * Server for processing JSON commands.
 */
public final class DataflowServer extends AbstractServer<DataflowServer> {
	public static final Version VERSION = Version.newBuilder().setMajor(1).setMinor(0).build();

	private static final int MAX_LAST_RAN_TASKS = ApplicationSettings.getInt(DataflowServer.class, "maxLastRanTasks", 1000);

	private final Map<StreamId, ChannelQueue<ByteBuf>> pendingStreams = new HashMap<>();

	private final ByteBufsCodec<DataflowRequest, DataflowResponse> codec;
	private final BinarySerializerLocator serializers;
	private final ResourceLocator environment;
	private final FunctionSerializer functionSerializer;

	private final Map<Long, Task> runningTasks = new HashMap<>();
	private final Map<Long, Task> lastTasks = new LinkedHashMap<>() {
		@Override
		protected boolean removeEldestEntry(Map.Entry eldest) {
			return size() > MAX_LAST_RAN_TASKS;
		}
	};

	private int succeededTasks = 0, canceledTasks = 0, failedTasks = 0;
	private Function<DataflowRequest.Handshake, DataflowResponse.Handshake> handshakeHandler = $ -> DataflowResponse.Handshake.newBuilder()
			.setOk(Ok.newBuilder().build())
			.build();

	private DataflowServer(Eventloop eventloop, ByteBufsCodec<DataflowRequest, DataflowResponse> codec, BinarySerializerLocator serializers, ResourceLocator environment, FunctionSerializer functionSerializer) {
		super(eventloop);
		this.codec = codec;
		this.serializers = serializers;
		this.environment = environment;
		this.functionSerializer = functionSerializer;
	}

	public static DataflowServer create(Eventloop eventloop, ByteBufsCodec<DataflowRequest, DataflowResponse> codec, BinarySerializerLocator serializers, ResourceLocator environment, FunctionSerializer functionSerializer) {
		return new DataflowServer(eventloop, codec, serializers, environment, functionSerializer);
	}

	public DataflowServer withHandshakeHandler(Function<DataflowRequest.Handshake, DataflowResponse.Handshake> handshakeHandler) {
		this.handshakeHandler = handshakeHandler;
		return this;
	}

	private void sendResponse(Messaging<DataflowRequest, DataflowResponse> messaging, @Nullable Exception exception) {
		String error = null;
		if (exception != null) {
			error = exception.getClass().getSimpleName() + ": " + exception.getMessage();
		}
		messaging.send(resultResponse(error))
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
		Messaging<DataflowRequest, DataflowResponse> messaging = MessagingWithBinaryStreaming.create(socket, codec);
		messaging.receive()
				.then(handshakeMsg -> {
					if (!handshakeMsg.hasHandshake()) {
						return Promise.ofException(new DataflowException("Handshake expected"));
					}
					DataflowResponse handshakeResponse = DataflowResponse.newBuilder()
							.setHandshake(handshakeHandler.apply(handshakeMsg.getHandshake()))
							.build();
					return messaging.send(handshakeResponse);
				})
				.then(messaging::receive)
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

	private void doRead(Messaging<DataflowRequest, DataflowResponse> messaging, DataflowRequest request) throws DataflowException {
		switch (request.getRequestCase()) {
			case DOWNLOAD -> handleDownload(messaging, request.getDownload());
			case EXECUTE -> handleExecute(messaging, request.getExecute());
			case GET_TASKS -> handleGetTasks(messaging, request.getGetTasks());
			case HANDSHAKE -> throw new DataflowException("Handshake was already performed");
			default -> {
				logger.error("missing handler for {}", request.getRequestCase());
				messaging.close();
			}
		}
	}

	private void handleDownload(Messaging<DataflowRequest, DataflowResponse> messaging, Download download) {
		if (logger.isTraceEnabled()) {
			logger.trace("Processing onDownload: {}, {}", download, messaging);
		}
		StreamId streamId = convert(download.getStreamId());
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
		consumer.withAcknowledgement(ack -> ack
				.whenComplete(messaging::close)
				.whenException(e -> logger.warn("Exception occurred while trying to send data", e))
		);
	}

	private void handleExecute(Messaging<DataflowRequest, DataflowResponse> messaging, Execute execute) throws DataflowException {
		long taskId = execute.getTaskId();
		Task task = new Task(taskId, environment, convert(execute.getNodesList(), functionSerializer));
		try {
			task.bind();
		} catch (Exception e) {
			logger.error("Failed to construct task: {}", execute, e);
			sendResponse(messaging, e);
			return;
		}
		lastTasks.put(taskId, task);
		runningTasks.put(taskId, task);
		task.execute()
				.whenComplete(($, exception) -> {
					runningTasks.remove(taskId);
					if (exception == null) {
						succeededTasks++;
						logger.info("Task executed successfully: {}", execute);
					} else if (exception instanceof AsyncCloseException) {
						canceledTasks++;
						logger.error("Canceled task: {}", execute, exception);
					} else {
						failedTasks++;
						logger.error("Failed to execute task: {}", execute, exception);
					}
					sendResponse(messaging, exception);
				});

		messaging.receive()
				.whenException(() -> {
					if (!task.isExecuted()) {
						logger.error("Client disconnected. Canceling task: {}", execute);
						task.cancel();
					}
				});
	}

	private void handleGetTasks(Messaging<DataflowRequest, DataflowResponse> messaging, GetTasks getTasks) {
		TaskId taskId = getTasks.getTaskId();
		if (taskId.getTaskIdIsNull()) {
			messaging.send(partitionDataResponse()).whenException(e -> logger.error("Failed to send answer for the partition data request", e));
			return;
		}
		Task task = lastTasks.get(taskId.getTaskId());
		if (task == null) {
			messaging.send(resultResponse("No task found with id " + taskId));
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
		messaging.send(taskDataResponse(task, err))
				.whenException(e -> logger.error("Failed to send answer for the task (" + taskId + ") data request", e));
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

	private static DataflowResponse resultResponse(@Nullable String error) {
		return DataflowResponse.newBuilder()
				.setResult(DataflowResponse.Result.newBuilder()
						.setError(error(error)))
				.build();
	}

	private DataflowResponse partitionDataResponse() {
		return DataflowResponse.newBuilder()
				.setPartitionData(DataflowResponse.PartitionData.newBuilder()
						.setRunning(getRunningTasks())
						.setCancelled(getCanceledTasks())
						.setFailed(getFailedTasks())
						.setSucceeded(getSucceededTasks())
						.addAllLast(lastTasks.entrySet().stream()
								.map(e -> TaskDesc.newBuilder()
										.setId(e.getKey())
										.setStatus(convert(e.getValue().getStatus()))
										.build())
								.collect(toList())))
				.build();
	}

	private DataflowResponse taskDataResponse(Task task, @Nullable String error) {
		return DataflowResponse.newBuilder()
				.setTaskData(TaskData.newBuilder()
						.setStatus(convert(task.getStatus()))
						.setStartTime(convert(task.getStartTime()))
						.setFinishTime(convert(task.getFinishTime()))
						.setError(error(error))
						.setGraphViz(task.getGraphViz())
						.putAllNodes(task.getNodes().stream()
								.filter(n -> n.getStats() != null)
								.collect(toMap(Node::getIndex, node -> convert(node.getStats())))))
				.build();
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
