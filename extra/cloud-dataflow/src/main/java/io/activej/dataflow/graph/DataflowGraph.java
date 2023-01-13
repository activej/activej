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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.activej.async.process.AsyncCloseable;
import io.activej.common.collection.Try;
import io.activej.common.ref.RefInt;
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.DataflowClient.Session;
import io.activej.dataflow.exception.DataflowException;
import io.activej.dataflow.node.Node;
import io.activej.dataflow.node.Node_Download;
import io.activej.dataflow.node.Node_Upload;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.ImplicitlyReactive;
import io.activej.reactor.Reactor;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.util.stream.Collectors.*;

/**
 * Represents a graph of partitions, nodeStats and streams in datagraph system.
 */
public final class DataflowGraph extends AbstractReactive {
	private static final ObjectWriter OBJECT_WRITER = new ObjectMapper()
			.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
			.writerFor(new TypeReference<List<Node>>() {})
			.with(new DefaultPrettyPrinter("%n"));

	private final Map<Node, Partition> nodePartitions = new LinkedHashMap<>();
	private final Map<StreamId, Node> streams = new LinkedHashMap<>();

	private final DataflowClient client;
	private final List<Partition> availablePartitions;

	public DataflowGraph(Reactor reactor, DataflowClient client, List<Partition> availablePartitions) {
		super(reactor);
		this.client = client;
		this.availablePartitions = availablePartitions;
	}

	public List<Partition> getAvailablePartitions() {
		return availablePartitions;
	}

	public Partition getPartition(Node node) {
		return nodePartitions.get(node);
	}

	public Partition getPartition(StreamId streamId) {
		return getPartition(streams.get(streamId));
	}

	private Map<Partition, List<Node>> getNodesByPartition() {
		return nodePartitions.entrySet().stream()
				.collect(groupingBy(Map.Entry::getValue, mapping(Map.Entry::getKey, toList())));
	}

	private static class PartitionSession extends ImplicitlyReactive implements AsyncCloseable {
		private final Partition partition;
		private final Session session;

		private PartitionSession(Partition partition, Session session) {
			this.partition = partition;
			this.session = session;
		}

		public Promise<Void> execute(long taskId, List<Node> nodes) {
			return session.execute(taskId, nodes);
		}

		@Override
		public void closeEx(Exception e) {
			session.closeEx(e);
		}
	}

	/**
	 * Executes the defined operations on all partitions.
	 */
	public Promise<Void> execute() {
		checkInReactorThread(this);
		Map<Partition, List<Node>> nodesByPartition = getNodesByPartition();
		long taskId = ThreadLocalRandom.current().nextInt() & (Integer.MAX_VALUE >>> 1);
		return connect(nodesByPartition.keySet())
				.then(sessions ->
						Promises.all(
										sessions.stream()
												.map(session -> session.execute(taskId, nodesByPartition.get(session.partition))))
								.whenException(() -> sessions.forEach(PartitionSession::close)));
	}

	private Promise<List<PartitionSession>> connect(Set<Partition> partitions) {
		return Promises.toList(partitions.stream()
						.map(partition -> client.connect(partition.getAddress()).map(session -> new PartitionSession(partition, session)).toTry()))
				.map(tries -> {
					List<PartitionSession> sessions = tries.stream()
							.filter(Try::isSuccess)
							.map(Try::get)
							.collect(toList());

					if (sessions.size() != partitions.size()) {
						sessions.forEach(PartitionSession::close);
						throw new DataflowException("Cannot connect to all partitions");
					}
					return sessions;
				});
	}

	public void addNode(Partition partition, Node node) {
		nodePartitions.put(node, partition);
		for (StreamId streamId : node.getOutputs()) {
			streams.put(streamId, node);
		}
	}

	public void addNodeStream(Node node, StreamId streamId) {
		streams.put(streamId, node);
	}

	public List<Partition> getPartitions(List<? extends StreamId> channels) {
		List<Partition> partitions = new ArrayList<>();
		for (StreamId streamId : channels) {
			Partition partition = getPartition(streamId);
			partitions.add(partition);
		}
		return partitions;
	}

	public String toGraphViz() {
		return toGraphViz(false, -1);
	}

	public String toGraphViz(boolean streamLabels) {
		return toGraphViz(streamLabels, -1);
	}

	public String toGraphViz(int maxPartitions) {
		return toGraphViz(false, maxPartitions);
	}

	@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
	public String toGraphViz(boolean streamLabels, int maxPartitions) {
		StringBuilder sb = new StringBuilder("digraph {\n\n");

		RefInt nodeCounter = new RefInt(0);
		RefInt clusterCounter = new RefInt(0);

		Map<StreamId, Node> nodesByInput = new HashMap<>();
		Map<StreamId, StreamId> network = new HashMap<>();

		List<Node_Upload<?>> uploads = new ArrayList<>();

		// collect network streams and populate the nodesByInput lookup map
		for (Node node : nodePartitions.keySet()) {
			if (node instanceof Node_Download<?> download) {
				network.put(download.getStreamId(), download.getOutput());
			} else if (node instanceof Node_Upload) {
				uploads.add((Node_Upload<?>) node);
			} else {
				node.getInputs().forEach(input -> nodesByInput.put(input, node));
			}
		}
		// check for upload nodeStats not connected to download ones, add them
		for (Node_Upload<?> upload : uploads) {
			StreamId streamId = upload.getStreamId();
			if (!network.containsKey(streamId)) {
				nodesByInput.put(streamId, upload);
			}
		}

		Map<Node, String> nodeIds = new HashMap<>();

		// define nodeStats and group them by partitions using graphviz clusters
		getNodesByPartition()
				.entrySet()
				.stream()
				.limit(maxPartitions == -1 ? availablePartitions.size() : maxPartitions)
				.forEach(e -> {
					sb.append("  subgraph cluster_")
							.append(++clusterCounter.value)
							.append(" {\n")
							.append("    label=\"")
							.append(e.getKey().getAddress())
							.append("\";\n    style=rounded;\n\n");
					for (Node node : e.getValue()) {
						// upload and download nodeStats have no common connections
						// download nodeStats are never drawn, and upload only has an input
						if ((node instanceof Node_Download || (node instanceof Node_Upload && network.containsKey(((Node_Upload<?>) node).getStreamId())))) {
							continue;
						}
						String nodeId = "n" + ++nodeCounter.value;
						String name = node.getClass().getSimpleName();
						sb.append("    ")
								.append(nodeId)
								.append(" [label=\"")
								.append(name.startsWith("Node") ? name.substring(4) : name)
								.append("\"];\n");
						nodeIds.put(node, nodeId);
					}
					sb.append("  }\n\n");
				});

		Set<String> notFound = new HashSet<>();

		// walk over each node outputs and build the connections
		nodeIds.forEach((node, id) -> {
			for (StreamId output : node.getOutputs()) {
				Node outputNode = nodesByInput.get(output);
				boolean net = false;
				boolean forceLabel = false;
				// check if unbound stream is a network one
				StreamId prev = null;
				if (outputNode == null) {
					StreamId through = network.get(output);
					if (through != null) {
						prev = output;
						output = through;
						// connect to network stream output and
						// set the 'net' flag to true for dashed arrow
						outputNode = nodesByInput.get(output);
						net = outputNode != null;
					}
				}
				String nodeId = nodeIds.get(outputNode);
				// if still unbound and not net (because of partition limit),
				// draw as point and force the stream label
				if (nodeId == null && !net) {
					nodeId = "s" + output.getId();
					notFound.add(nodeId);
					forceLabel = true;
				}
				if (nodeId != null) { // nodeId might be null only for net nodeStats here, see previous 'if'
					sb.append("  ")
							.append(id)
							.append(" -> ")
							.append(nodeId)
							.append(" [");
					if (streamLabels || forceLabel) {
						if (prev != null) {
							sb.append("taillabel=\"")
									.append(prev)
									.append("\", headlabel=\"")
									.append(output)
									.append("\"");
						} else {
							sb.append("xlabel=\"")
									.append(output)
									.append("\"");
						}
						if (net) {
							sb.append(", ");
						}
					}
					if (net) {
						sb.append("style=dashed");
					}
					sb.append("];\n");
				}
			}
		});

		// draw the nodeStats that were never defined as points that still have connections
		if (!notFound.isEmpty()) {
			sb.append('\n');
			notFound.forEach(id -> sb.append("  " + id + " [shape=point];\n"));
		}

		sb.append("}");

		return sb.toString();
	}

	@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Map<Partition, List<Node>> map = getNodesByPartition();
		for (Map.Entry<Partition, List<Node>> entry : map.entrySet()) {
			sb.append("--- " + entry.getKey() + "\n\n");
			String nodes;
			try {
				nodes = OBJECT_WRITER.writeValueAsString(entry.getValue());
			} catch (JsonProcessingException e) {
				nodes = "<UNKNOWN>";
			}
			sb.append(nodes);
			sb.append("\n\n");
		}
		return sb.toString();
	}
}
