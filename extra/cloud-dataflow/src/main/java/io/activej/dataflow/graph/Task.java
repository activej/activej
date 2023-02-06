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

import io.activej.async.exception.AsyncCloseException;
import io.activej.dataflow.exception.DataflowException;
import io.activej.dataflow.inject.DatasetIdModule.DatasetIds;
import io.activej.dataflow.node.Node;
import io.activej.dataflow.node.impl.Download;
import io.activej.dataflow.node.impl.Upload;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.supplier.StreamSupplier;
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

	private @Nullable Instant started;
	private @Nullable Instant finished;
	private @Nullable Exception error;

	private @Nullable List<Promise<Void>> currentNodeAcks;

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
						return Promise.ofException(new DataflowException(e));
					}
				}).collect(toList()))
				.whenComplete(($, e) -> {
					finished = Instant.now();
					if (e != null && !(e instanceof DataflowException)) {
						error = new DataflowException(e);
					} else {
						error = e;
					}
					status = e == null ?
							TaskStatus.COMPLETED :
							e instanceof AsyncCloseException ?
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

	public @Nullable Instant getStartTime() {
		return started;
	}

	public @Nullable Instant getFinishTime() {
		return finished;
	}

	public @Nullable Exception getError() {
		return error;
	}

	public List<Node> getNodes() {
		return nodes;
	}

	public long getTaskId() {
		return taskId;
	}

	@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
	@JmxAttribute
	public String getGraphViz() {
		Map<StreamId, Node> nodesByInput = new HashMap<>();
		Map<StreamId, Node> nodesByOutput = new HashMap<>();
		Map<StreamId, Node> uploads = new HashMap<>();
		Map<StreamId, StreamId> network = new HashMap<>();
		Map<Node, String> ids = new LinkedHashMap<>();

		for (Node node : nodes) {
			if (node instanceof Download<?> download) {
				network.put(download.streamId, download.output);
			} else if (node instanceof Upload) {
				uploads.put(((Upload<?>) node).streamId, node);
			} else {
				node.getInputs().forEach(input -> nodesByInput.put(input, node));
				node.getOutputs().forEach(input -> nodesByOutput.put(input, node));
			}
		}
		StringBuilder sb = new StringBuilder("digraph {\n");
		for (Node node : nodes) {
			if (node instanceof Download) {
				StreamId input = ((Download<?>) node).streamId;
				if (nodesByOutput.containsKey(input)) {
					continue;
				}
				Node target = nodesByInput.get(((Download<?>) node).output);
				if (target != null) {
					// DOWNLOAD POINT HERE
					String nodeId = "n" + node.getIndex();
					sb.append("  " + nodeId + " [id=\"" + nodeId + "\", shape=point, xlabel=\"" + input.getId() + "\"]\n");
					sb.append("  " + nodeId + " -> " + ids.computeIfAbsent(target, NODE_ID_FUNCTION) + " [style=dashed]\n");
				}
				continue;
			} else if (node instanceof Upload) {
				continue;
			}
			String nodeId = ids.computeIfAbsent(node, NODE_ID_FUNCTION);
			for (StreamId output : node.getOutputs()) {
				Node target = nodesByInput.get(output);
				if (target != null) {
					sb.append("  " + nodeId + " -> " + ids.computeIfAbsent(target, NODE_ID_FUNCTION) + "\n");
					continue;
				}
				Node netTarget = nodesByInput.get(network.get(output));
				if (netTarget != null) {
					sb.append("  " + nodeId + " -> " + ids.computeIfAbsent(netTarget, NODE_ID_FUNCTION));
				} else {
					// UPLOAD POINT HERE
					Node test = uploads.get(output);
					String outputId = test != null ? "n" + test.getIndex() : "s" + output.getId();
					sb.append("  " + outputId + " [id=\"" + outputId + "\", shape=point, xlabel=\"" + output.getId() + "\"]\n");
					sb.append("  " + nodeId + " -> " + outputId);
				}
				sb.append(" [style=dashed]\n");
			}
		}
		sb.append('\n');
		ids.forEach((node, id) -> {
			String name = node.getClass().getSimpleName();
			sb.append("  " + id)
					.append(" [label=\"" + name)
					.append("\" id=" + id);
			Exception error = node.getError();
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
