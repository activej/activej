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

package io.activej.dataflow.dataset.impl;

import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.DatasetUtils;
import io.activej.dataflow.graph.*;
import io.activej.dataflow.node.Node;
import io.activej.dataflow.node.Nodes;
import io.activej.dataflow.node.impl.Reduce;
import io.activej.dataflow.node.impl.ReduceSimple;
import io.activej.dataflow.node.impl.Shard;
import io.activej.datastream.processor.StreamReducers.ReducerToResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static io.activej.dataflow.dataset.DatasetUtils.generateIndexes;

public final class DatasetSplitSortReduceRepartitionReduce<K, I, O, A> extends Dataset<O> {
	private final Dataset<I> input;
	private final Function<I, K> inputKeyFunction;
	private final Function<A, K> accumulatorKeyFunction;
	private final Comparator<K> keyComparator;
	private final ReducerToResult<K, I, O, A> reducer;
	private final StreamSchema<A> accumulatorStreamSchema;
	private final int sortBufferSize;

	public DatasetSplitSortReduceRepartitionReduce(Dataset<I> input,
			Function<I, K> inputKeyFunction,
			Function<A, K> accumulatorKeyFunction,
			Comparator<K> keyComparator,
			ReducerToResult<K, I, O, A> reducer,
			StreamSchema<O> resultStreamSchema,
			StreamSchema<A> accumulatorStreamSchema,
			int sortBufferSize) {
		super(resultStreamSchema);
		this.input = input;
		this.inputKeyFunction = inputKeyFunction;
		this.accumulatorKeyFunction = accumulatorKeyFunction;
		this.keyComparator = keyComparator;
		this.reducer = reducer;
		this.accumulatorStreamSchema = accumulatorStreamSchema;
		this.sortBufferSize = sortBufferSize;
	}

	@Override
	public List<StreamId> channels(DataflowContext context) {
		DataflowGraph graph = context.getGraph();
		int nonce = context.getNonce();
		List<StreamId> outputStreamIds = new ArrayList<>();
		List<Shard<K, I>> sharders = new ArrayList<>();
		int shardIndex = context.generateNodeIndex();
		for (StreamId inputStreamId : input.channels(context.withoutFixedNonce())) {
			Partition partition = graph.getPartition(inputStreamId);
			Shard<K, I> sharder = Shard.create(shardIndex, inputKeyFunction, inputStreamId, nonce);
			graph.addNode(partition, sharder);
			sharders.add(sharder);
		}

		int reduceIndex = context.generateNodeIndex();
		List<Partition> partitions = graph.getAvailablePartitions();
		int[] uploadIndexes = generateIndexes(context, sharders.size());
		int[] downloadIndexes = generateIndexes(context, partitions.size());
		for (int i = 0; i < partitions.size(); i++) {
			Partition partition = partitions.get(i);
			Reduce<K, O, A>.Builder nodeReduceBuilder = Reduce.builder(reduceIndex, keyComparator);

			int sortIndex = context.generateNodeIndex();
			int simpleReduceIndex = context.generateNodeIndex();
			for (int j = 0; j < sharders.size(); j++) {
				Shard<K, I> sharder = sharders.get(j);
				StreamId sharderOutput = sharder.newPartition();
				graph.addNodeStream(sharder, sharderOutput);
				StreamId reducerInput = sortReduceForward(context, sharderOutput, partition, sortIndex, simpleReduceIndex, uploadIndexes[i], downloadIndexes[j]);
				nodeReduceBuilder.withInput(reducerInput, accumulatorKeyFunction, reducer.accumulatorToOutput());
			}
			Reduce<K, O, A> nodeReduce = nodeReduceBuilder.build();
			graph.addNode(partition, nodeReduce);

			outputStreamIds.add(nodeReduce.output);
		}

		return outputStreamIds;
	}

	private StreamId sortReduceForward(DataflowContext context, StreamId sourceStreamId, Partition targetPartition, int sortIndex, int simpleReduceIndex, int uploadIndex, int downloadIndex) {
		DataflowGraph graph = context.getGraph();
		Partition sourcePartition = graph.getPartition(sourceStreamId);

		Node nodeSort = Nodes.sort(sortIndex, input.streamSchema(), inputKeyFunction, keyComparator, false, sortBufferSize, sourceStreamId);
		graph.addNode(sourcePartition, nodeSort);

		ReduceSimple<K, I, A, A>.Builder nodeReduceBuilder = ReduceSimple.builder(simpleReduceIndex, inputKeyFunction, keyComparator, reducer.inputToAccumulator());
		for (StreamId output : nodeSort.getOutputs()) {
			nodeReduceBuilder.withInput(output);
		}
		ReduceSimple<K, I, A, A> nodeReduce = nodeReduceBuilder.build();
		graph.addNode(sourcePartition, nodeReduce);

		return DatasetUtils.forwardChannel(context, accumulatorStreamSchema, nodeReduce.output, targetPartition, uploadIndex, downloadIndex);
	}

	@Override
	public Collection<Dataset<?>> getBases() {
		return List.of(input);
	}
}
