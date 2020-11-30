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

package io.activej.dataflow.graph;

import io.activej.common.exception.CloseException;
import io.activej.dataflow.inject.DatasetIdModule.DatasetIds;
import io.activej.dataflow.node.Node;
import io.activej.dataflow.node.NodeDownload;
import io.activej.dataflow.node.NodeUpload;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.inject.Key;
import io.activej.inject.ResourceLocator;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.Nullable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.Checks.checkState;
import static java.util.stream.Collectors.toList;

/**
 * Represents a context of a datagraph system: environment, suppliers and
 * consumers.
 * Provides functionality to alter context and wire components.
 */
public final class Task {
	private final Map<StreamId, StreamSupplier<?>> suppliers = new LinkedHashMap<>();
	private final Map<StreamId, StreamConsumer<?>> consumers = new LinkedHashMap<>();
	private final SettablePromise<Void> executionPromise = new SettablePromise<>();

	private final long taskId;
	private final ResourceLocator environment;
	private final DatasetIds datasetIds;
	private final List<Node> nodes;

	private final AtomicBoolean bound = new AtomicBoolean();

	private TaskStatus status = TaskStatus.RUNNING;

	@Nullable
	private Instant started;
	@Nullable
	private Instant finished;
	@Nullable
	private Throwable error;

	@Nullable
	private List<Promise<Void>> currentNodeAcks;

	public Task(long taskId, ResourceLocator environment, List<Node> nodes) {
		this.taskId = taskId;
		this.environment = environment;
		this.nodes = nodes;
		this.datasetIds = environment.getInstance(DatasetIds.class);
	}

	public void bind() {
		if (!bound.compareAndSet(false, true)) {
			throw new IllegalStateException("Task was already bound!");
		}
		for (Node node : nodes) {
			currentNodeAcks = new ArrayList<>();
			node.createAndBind(this);
			Promises.all(currentNodeAcks).whenComplete(($, e) -> node.finish(e));
		}
		currentNodeAcks = null;
	}

	public Object get(String key) {
		return environment.getInstance(datasetIds.getKeyForId(key));
	}

	public <T> T get(Class<T> cls) {
		return environment.getInstance(cls);
	}

	public <T> T get(Key<T> key) {
		return environment.getInstance(key);
	}

	public <T> void bindChannel(StreamId streamId, StreamConsumer<T> consumer) {
		checkState(!consumers.containsKey(streamId), "Already bound");
		checkState(currentNodeAcks != null, "Must bind streams only from createAndBind");
		consumers.put(streamId, consumer);
		currentNodeAcks.add(consumer.getAcknowledgement());
	}

	public <T> void export(StreamId streamId, StreamSupplier<T> supplier) {
		checkState(!suppliers.containsKey(streamId), "Already exported");
		checkState(currentNodeAcks != null, "Must bind streams only from createAndBind");
		suppliers.put(streamId, supplier);
		currentNodeAcks.add(supplier.getAcknowledgement());
	}

	@SuppressWarnings("unchecked")
	public Promise<Void> execute() {
		started = Instant.now();
		return Promises.all(suppliers.entrySet().stream().map(supplierEntry -> {
			StreamId streamId = supplierEntry.getKey();
			try {
				StreamSupplier<Object> supplier = (StreamSupplier<Object>) supplierEntry.getValue();
				StreamConsumer<Object> consumer = (StreamConsumer<Object>) consumers.get(streamId);
				checkNotNull(supplier, "Supplier not found for %s, consumer %s", streamId, consumer);
				checkNotNull(consumer, "Consumer not found for %s, supplier %s", streamId, supplier);

				return supplier.streamTo(consumer);
			} catch (Exception e) {
				return Promise.ofException(e);
			}
		}).collect(toList()))
				.whenComplete(($, e) -> {
					finished = Instant.now();
					error = e;
					status = e == null ?
							TaskStatus.COMPLETED :
							e instanceof CloseException ?
									TaskStatus.CANCELED :
									TaskStatus.FAILED;
					executionPromise.accept($, e);
				});
	}

	public void cancel() {
		suppliers.values().forEach(StreamSupplier::close);
		consumers.values().forEach(StreamConsumer::close);
	}

	public Promise<Void> getExecutionPromise() {
		return executionPromise;
	}

	public boolean isExecuted() {
		return executionPromise.isComplete();
	}

	public TaskStatus getStatus() {
		return status;
	}

	@Nullable
	public Instant getStartTime() {
		return started;
	}

	@Nullable
	public Instant getFinishTime() {
		return finished;
	}

	@Nullable
	public Throwable getError() {
		return error;
	}

	public List<Node> getNodes() {
		return nodes;
	}

	public long getTaskId() {
		return taskId;
	}

	@JmxAttribute
	public String getGraphViz() {
		Map<StreamId, Node> nodesByInput = new HashMap<>();
		Map<StreamId, Node> nodesByOutput = new HashMap<>();
		Map<StreamId, Node> uploads = new HashMap<>();
		Map<StreamId, StreamId> network = new HashMap<>();
		Map<Node, String> ids = new LinkedHashMap<>();

		for (Node node : nodes) {
			if (node instanceof NodeDownload) {
				NodeDownload<?> download = (NodeDownload<?>) node;
				network.put(download.getStreamId(), download.getOutput());
			} else if (node instanceof NodeUpload) {
				uploads.put(((NodeUpload<?>) node).getStreamId(), node);
			} else {
				node.getInputs().forEach(input -> nodesByInput.put(input, node));
				node.getOutputs().forEach(input -> nodesByOutput.put(input, node));
			}
		}
		StringBuilder sb = new StringBuilder("digraph {\n");
		for (Node node : nodes) {
			if (node instanceof NodeDownload) {
				StreamId input = ((NodeDownload<?>) node).getStreamId();
				if (nodesByOutput.containsKey(input)) {
					continue;
				}
				Node target = nodesByInput.get(((NodeDownload<?>) node).getOutput());
				if (target != null) {
					// DOWNLOAD POINT HERE
					String nodeId = "n" + node.getIndex();
					sb.append("  ").append(nodeId).append(" [id=\"").append(nodeId).append("\", shape=point, xlabel=\"").append(input.getId()).append("\"]\n");
					sb.append("  ").append(nodeId).append(" -> ").append(ids.computeIfAbsent(target, NODE_ID_FUNCTION)).append(" [style=dashed]\n");
				}
				continue;
			} else if (node instanceof NodeUpload) {
				continue;
			}
			String nodeId = ids.computeIfAbsent(node, NODE_ID_FUNCTION);
			for (StreamId output : node.getOutputs()) {
				Node target = nodesByInput.get(output);
				if (target != null) {
					sb.append("  ").append(nodeId).append(" -> ").append(ids.computeIfAbsent(target, NODE_ID_FUNCTION)).append('\n');
					continue;
				}
				Node netTarget = nodesByInput.get(network.get(output));
				if (netTarget != null) {
					sb.append("  ").append(nodeId).append(" -> ").append(ids.computeIfAbsent(netTarget, NODE_ID_FUNCTION));
				} else {
					// UPLOAD POINT HERE
					Node test = uploads.get(output);
					String outputId = test != null ? "n" + test.getIndex() : "s" + output.getId();
					sb.append("  ").append(outputId).append(" [id=\"").append(outputId).append("\", shape=point, xlabel=\"").append(output.getId()).append("\"]\n");
					sb.append("  ").append(nodeId).append(" -> ").append(outputId);
				}
				sb.append(" [style=dashed]\n");
			}
		}
		sb.append('\n');
		ids.forEach((node, id) -> {
			String name = node.getClass().getSimpleName();
			sb.append("  ").append(id)
					.append(" [label=\"").append(name.startsWith("Node") ? name.substring(4) : name)
					.append("\" id=").append(id);
			Throwable error = node.getError();
			if (error != null) {
				StringWriter str = new StringWriter();
				error.printStackTrace(new PrintWriter(str));
				sb.append(" color=red tooltip=\"")
						.append(str.toString().replace("\"", "\\\""))
						.append("\"");
			} else if (node.getFinished() != null) {
				sb.append(" color=blue");
			}
			sb.append("]\n");
		});
		return sb.append('}').toString();
	}

	private static final Function<Node, String> NODE_ID_FUNCTION = n -> "n" + n.getIndex();
}
