package io.activej.dataflow.helper;

import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.node.NodeUpload;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.promise.Promises;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.activej.common.Checks.checkState;

public final class PartitionedCollector<T> {
	private final Dataset<T> input;
	private final DataflowClient client;

	public PartitionedCollector(Dataset<T> input, DataflowClient client) {
		this.input = input;
		this.client = client;
	}

	public Promise<Map<Partition, List<T>>> compile(DataflowGraph graph) {
		Map<Partition, List<T>> result = new LinkedHashMap<>();

		List<Promise<Void>> streamingPromises = new ArrayList<>();
		for (StreamId streamId : input.channels(DataflowContext.of(graph))) {
			NodeUpload<String> nodeUpload = new NodeUpload<>(0, String.class, streamId);
			Partition partition = graph.getPartition(streamId);
			graph.addNode(partition, nodeUpload);
			StreamSupplier<T> supplier = client.download(partition.getAddress(), streamId, input.valueType());
			ArrayList<T> partitionItems = new ArrayList<>();
			List<T> prev = result.put(partition, partitionItems);
			checkState(prev == null, "Partition provides multiple channels");
			streamingPromises.add(supplier.streamTo(StreamConsumerToList.create(partitionItems)));
		}
		return Promises.all(streamingPromises)
				.map($ -> result);
	}
}
