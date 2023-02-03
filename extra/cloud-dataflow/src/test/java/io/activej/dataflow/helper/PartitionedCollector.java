package io.activej.dataflow.helper;

import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.collector.ICollector;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.graph.*;
import io.activej.dataflow.node.Node;
import io.activej.dataflow.node.Nodes;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.ToListStreamConsumer;
import io.activej.promise.Promise;
import io.activej.promise.Promises;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.activej.common.Checks.checkState;

public final class PartitionedCollector<T> implements ICollector<T> {
	private final Dataset<T> input;
	private final DataflowClient client;
	private final Map<Partition, List<T>> result = new LinkedHashMap<>();

	public PartitionedCollector(Dataset<T> input, DataflowClient client) {
		this.input = input;
		this.client = client;
	}

	public Map<Partition, List<T>> getResult() {
		return result;
	}

	public StreamSupplier<T> compile(DataflowGraph graph) {
		List<Promise<Void>> streamingPromises = new ArrayList<>();
		for (StreamId streamId : input.channels(DataflowContext.of(graph))) {
			Node nodeUpload = Nodes.upload(0, StreamSchemas.simple(String.class), streamId);
			Partition partition = graph.getPartition(streamId);
			graph.addNode(partition, nodeUpload);
			StreamSupplier<T> supplier = client.download(partition.address(), streamId, input.streamSchema());
			ArrayList<T> partitionItems = new ArrayList<>();
			List<T> prev = result.put(partition, partitionItems);
			checkState(prev == null, "Partition provides multiple channels");
			streamingPromises.add(supplier.streamTo(ToListStreamConsumer.create(partitionItems)));
		}
		return StreamSupplier.ofPromise(Promises.all(streamingPromises)
				.map($ -> StreamSupplier.closing()));
	}
}
