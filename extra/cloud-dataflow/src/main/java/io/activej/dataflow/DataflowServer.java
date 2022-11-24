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
import io.activej.common.exception.TruncatedDataException;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.dsl.ChannelTransformer;
import io.activej.csp.net.Messaging;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.csp.queue.ChannelQueue;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.dataflow.exception.DataflowException;
import io.activej.dataflow.exception.DataflowStacklessException;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.graph.Task;
import io.activej.dataflow.inject.BinarySerializerModule.BinarySerializerLocator;
import io.activej.dataflow.messaging.DataflowRequest;
import io.activej.dataflow.messaging.DataflowRequest.Download;
import io.activej.dataflow.messaging.DataflowRequest.Execute;
import io.activej.dataflow.messaging.DataflowRequest.GetTasks;
import io.activej.dataflow.messaging.DataflowResponse;
import io.activej.dataflow.messaging.DataflowResponse.Result;
import io.activej.dataflow.messaging.DataflowResponse.TaskData;
import io.activej.dataflow.messaging.DataflowResponse.TaskDescription;
import io.activej.dataflow.messaging.Version;
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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * Server for processing JSON commands.
 */
public final class DataflowServer extends AbstractServer<DataflowServer> {
	public static final Version VERSION = new Version(1, 0);

	private static final int MAX_LAST_RAN_TASKS = ApplicationSettings.getInt(DataflowServer.class, "maxLastRanTasks", 1000);

	private final Map<StreamId, ChannelQueue<ByteBuf>> pendingStreams = new HashMap<>();

	private final ByteBufsCodec<DataflowRequest, DataflowResponse> codec;
	private final BinarySerializerLocator serializers;
	private final ResourceLocator environment;

	private final Map<Long, Task> runningTasks = new HashMap<>();
	private final Map<Long, Task> lastTasks = new LinkedHashMap<>() {
		@Override
		protected boolean removeEldestEntry(Map.Entry eldest) {
			return size() > MAX_LAST_RAN_TASKS;
		}
	};

	private int succeededTasks = 0, canceledTasks = 0, failedTasks = 0;
	private Function<DataflowRequest.Handshake, DataflowResponse.Handshake> handshakeHandler = $ ->
			new DataflowResponse.Handshake(null);

	private DataflowServer(Eventloop eventloop, ByteBufsCodec<DataflowRequest, DataflowResponse> codec, BinarySerializerLocator serializers, ResourceLocator environment) {
		super(eventloop);
		this.codec = codec;
		this.serializers = serializers;
		this.environment = environment;
	}

	public static DataflowServer create(Eventloop eventloop, ByteBufsCodec<DataflowRequest, DataflowResponse> codec, BinarySerializerLocator serializers, ResourceLocator environment) {
		return new DataflowServer(eventloop, codec, serializers, environment);
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
		messaging.send(new Result(error))
				.whenComplete(messaging::close);
	}

	public <T> StreamConsumer<T> upload(StreamId streamId, StreamSchema<T> streamSchema, ChannelTransformer<ByteBuf, ByteBuf> transformer) {
		ChannelSerializer<T> streamSerializer = ChannelSerializer.create(streamSchema.createSerializer(serializers))
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
		return streamSerializer
				.withAcknowledgement(ack -> ack.mapException(IOException.class, DataflowStacklessException::new));
	}

	public <T> StreamConsumer<T> upload(StreamId streamId, StreamSchema<T> streamSchema) {
		return upload(streamId, streamSchema, ChannelTransformer.identity());
	}

	@Override
	protected void serve(AsyncTcpSocket socket, InetAddress remoteAddress) {
		Messaging<DataflowRequest, DataflowResponse> messaging = MessagingWithBinaryStreaming.create(socket, codec);
		messaging.receive()
				.map(request -> {
					if (!DataflowRequest.Handshake.class.isAssignableFrom(request.getClass())) {
						throw new DataflowException("Handshake expected, got: " + request);
					}
					return (DataflowRequest.Handshake) request;
				})
				.then(handshakeMsg -> messaging.send(handshakeHandler.apply(handshakeMsg)))
				.then(messaging::receive)
				.mapException(IOException.class, DataflowStacklessException::new)
				.mapException(TruncatedDataException.class, e -> new DataflowStacklessException(e.getMessage()))
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
		Class<? extends DataflowRequest> requestClass = request.getClass();
		if (requestClass == Download.class) handleDownload(messaging, (Download) request);
		else if (requestClass == Execute.class) handleExecute(messaging, (Execute) request);
		else if (requestClass == GetTasks.class) handleGetTasks(messaging, (GetTasks) request);
		else if (requestClass == DataflowRequest.Handshake.class)
			throw new DataflowException("Handshake was already performed");
	}

	private void handleDownload(Messaging<DataflowRequest, DataflowResponse> messaging, Download download) {
		if (logger.isTraceEnabled()) {
			logger.trace("Processing onDownload: {}, {}", download, messaging);
		}
		StreamId streamId = download.streamId();
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
		ChannelConsumer<ByteBuf> consumer = messaging.sendBinaryStream()
				.withAcknowledgement(ack -> ack
						.mapException(IOException.class, DataflowStacklessException::new));
		forwarder.getSupplier()
				.streamTo(consumer);
		consumer.withAcknowledgement(ack -> ack
				.whenComplete(messaging::close)
				.whenException(e -> logger.warn("Exception occurred while trying to send data", e))
		);
	}

	private void handleExecute(Messaging<DataflowRequest, DataflowResponse> messaging, Execute execute) {
		long taskId = execute.taskId();
		Task task = new Task(taskId, environment, execute.nodes());
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
				.mapException(IOException.class, DataflowStacklessException::new)
				.mapException(TruncatedDataException.class, e -> new DataflowStacklessException(e.getMessage()))
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
		Long taskId = getTasks.taskId();
		if (taskId == null) {
			messaging.send(partitionDataResponse())
					.mapException(IOException.class, DataflowStacklessException::new)
					.whenException(e -> logger.error("Failed to send answer for the partition data request", e));
			return;
		}
		Task task = lastTasks.get(taskId);
		if (task == null) {
			messaging.send(new Result("No task found with id " + taskId));
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
				.mapException(IOException.class, DataflowStacklessException::new)
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

	private DataflowResponse partitionDataResponse() {
		return new DataflowResponse.PartitionData(
				getRunningTasks(),
				getSucceededTasks(),
				getFailedTasks(),
				getCanceledTasks(),
				lastTasks.entrySet().stream()
						.map(e -> new TaskDescription(e.getKey(), e.getValue().getStatus()))
						.collect(toList())
		);
	}

	private DataflowResponse taskDataResponse(Task task, @Nullable String error) {
		return new TaskData(
				task.getStatus(),
				task.getStartTime(),
				task.getFinishTime(),
				error,
				task.getNodes().stream()
						.filter(n -> n.getStats() != null)
						.collect(toMap(Node::getIndex, Node::getStats)),
				task.getGraphViz()
		);
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
