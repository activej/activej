package io.activej.dataflow.dataset.impl;

import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.DatasetUtils;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.StreamId;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.function.Function;

public final class DatasetRepartition<T, K> extends Dataset<T> {
	private final Dataset<T> input;
	private final Function<T, K> keyFunction;
	private final @Nullable List<Partition> partitions;

	public DatasetRepartition(Dataset<T> input, Function<T, K> keyFunction, @Nullable List<Partition> partitions) {
		super(input.streamSchema());
		this.input = input;
		this.keyFunction = keyFunction;
		this.partitions = partitions;
	}

	@Override
	public List<StreamId> channels(DataflowContext context) {
		DataflowGraph graph = context.getGraph();
		List<StreamId> inputs = input.channels(context.withoutFixedNonce());
		List<Partition> partitions = this.partitions == null ? graph.getAvailablePartitions() : this.partitions;

		return DatasetUtils.repartition(context, inputs, input.streamSchema(), keyFunction, partitions);
	}
}
