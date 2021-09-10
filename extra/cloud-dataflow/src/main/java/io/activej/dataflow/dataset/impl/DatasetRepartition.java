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
import static io.activej.dataflow.dataset.DatasetUtils.generateIndexes;

public final class DatasetRepartition<T, K> extends Dataset<T> {
	private final Dataset<T> input;
	private final Function<T, K> keyFunction;
	private final @Nullable List<Partition> partitions;

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
		int shardIndex = context.generateNodeIndex();
		for (StreamId inputStreamId : input.channels(context.withoutFixedNonce())) {
			Partition partition = graph.getPartition(inputStreamId);
			NodeShard<K, T> sharder = new NodeShard<>(shardIndex, keyFunction, inputStreamId, nonce);
			graph.addNode(partition, sharder);
			sharders.add(sharder);
		}

		int unionIndex = context.generateNodeIndex();
		int[] downloadIndexes = generateIndexes(context, sharders.size());
		int[] uploadIndexes = generateIndexes(context, partitions.size());
		for (int i = 0; i < partitions.size(); i++) {
			Partition partition = partitions.get(i);
			List<StreamId> unionInputs = new ArrayList<>();
			for (int j = 0; j < sharders.size(); j++) {
				NodeShard<K, T> sharder = sharders.get(j);
				StreamId sharderOutput = sharder.newPartition();
				graph.addNodeStream(sharder, sharderOutput);
				StreamId unionInput = forwardChannel(context, input.valueType(), sharderOutput, partition, uploadIndexes[i], downloadIndexes[j]);
				unionInputs.add(unionInput);
			}
			NodeUnion<T> nodeUnion = new NodeUnion<>(unionIndex, unionInputs);
			graph.addNode(partition, nodeUnion);

			outputStreamIds.add(nodeUnion.getOutput());
		}

		return outputStreamIds;
	}
}
