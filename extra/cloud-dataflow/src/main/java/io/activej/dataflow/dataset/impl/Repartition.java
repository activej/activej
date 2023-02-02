package io.activej.dataflow.dataset.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.DatasetUtils;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.StreamId;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.function.Function;

@ExposedInternals
public final class Repartition<T, K> extends Dataset<T> {
	public final Dataset<T> input;
	public final Function<T, K> keyFunction;
	public final @Nullable List<Partition> partitions;

	public Repartition(Dataset<T> input, Function<T, K> keyFunction, @Nullable List<Partition> partitions) {
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
