package io.activej.dataflow.dataset.impl;

import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.node.NodeShard;
import io.activej.dataflow.node.NodeUnion;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static io.activej.dataflow.dataset.DatasetUtils.forwardChannel;

public final class DatasetRepartition<T, K> extends Dataset<T> {
	private final Dataset<T> input;
	private final Function<T, K> keyFunction;
	@Nullable
	private final List<Partition> partitions;

	public DatasetRepartition(Dataset<T> input, Function<T, K> keyFunction, @Nullable List<Partition> partitions) {
		super(input.valueType());
		this.input = input;
		this.keyFunction = keyFunction;
		this.partitions = partitions;
	}

	@Override
	public List<StreamId> channels(DataflowContext context) {
		DataflowGraph graph = context.getGraph();
		List<Partition> partitions = this.partitions == null ? graph.getAvailablePartitions() : this.partitions;

		int nonce = context.getNonce();
		List<StreamId> outputStreamIds = new ArrayList<>();

		List<NodeShard<K, T>> sharders = new ArrayList<>();
		for (StreamId inputStreamId : input.channels(context.withoutFixedNonce())) {
			Partition partition = graph.getPartition(inputStreamId);
			NodeShard<K, T> sharder = new NodeShard<>(context.generateNodeIndex(), keyFunction, inputStreamId, nonce);
			graph.addNode(partition, sharder);
			sharders.add(sharder);
		}

		for (Partition partition : partitions) {
			List<StreamId> unionInputs = new ArrayList<>();
			for (NodeShard<K, T> sharder : sharders) {
				StreamId sharderOutput = sharder.newPartition();
				graph.addNodeStream(sharder, sharderOutput);
				StreamId unionInput = forwardChannel(context, input.valueType(), sharderOutput, partition);
				unionInputs.add(unionInput);
			}
			NodeUnion<T> nodeUnion = new NodeUnion<>(context.generateNodeIndex(), unionInputs);
			graph.addNode(partition, nodeUnion);

			outputStreamIds.add(nodeUnion.getOutput());
		}

		return outputStreamIds;
	}
}
