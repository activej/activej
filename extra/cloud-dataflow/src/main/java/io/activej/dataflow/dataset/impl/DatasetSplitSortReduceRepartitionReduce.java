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
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.node.NodeReduce;
import io.activej.dataflow.node.NodeReduceSimple;
import io.activej.dataflow.node.NodeShard;
import io.activej.dataflow.node.NodeSort;
import io.activej.datastream.processor.StreamReducers.ReducerToResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static io.activej.dataflow.dataset.DatasetUtils.generateIndexes;
import static java.util.Collections.singletonList;

public final class DatasetSplitSortReduceRepartitionReduce<K, I, O, A> extends Dataset<O> {
	private final Dataset<I> input;
	private final Function<I, K> inputKeyFunction;
	private final Function<A, K> accumulatorKeyFunction;
	private final Comparator<K> keyComparator;
	private final ReducerToResult<K, I, O, A> reducer;
	private final Class<A> accumulatorType;
	private final int sortBufferSize;

	public DatasetSplitSortReduceRepartitionReduce(Dataset<I> input,
			Function<I, K> inputKeyFunction,
			Function<A, K> accumulatorKeyFunction,
			Comparator<K> keyComparator,
			ReducerToResult<K, I, O, A> reducer,
			Class<O> resultType,
			Class<A> accumulatorType,
			int sortBufferSize) {
		super(resultType);
		this.input = input;
		this.inputKeyFunction = inputKeyFunction;
		this.accumulatorKeyFunction = accumulatorKeyFunction;
		this.keyComparator = keyComparator;
		this.reducer = reducer;
		this.accumulatorType = accumulatorType;
		this.sortBufferSize = sortBufferSize;
	}

	@Override
	public List<StreamId> channels(DataflowContext context) {
		DataflowGraph graph = context.getGraph();
		int nonce = context.getNonce();
		List<StreamId> outputStreamIds = new ArrayList<>();
		List<NodeShard<K, I>> sharders = new ArrayList<>();
		int shardIndex = context.generateNodeIndex();
		for (StreamId inputStreamId : input.channels(context.withoutFixedNonce())) {
			Partition partition = graph.getPartition(inputStreamId);
			NodeShard<K, I> sharder = new NodeShard<>(shardIndex, inputKeyFunction, inputStreamId, nonce);
			graph.addNode(partition, sharder);
			sharders.add(sharder);
		}

		int reduceIndex = context.generateNodeIndex();
		List<Partition> partitions = graph.getAvailablePartitions();
		int[] uploadIndexes = generateIndexes(context, sharders.size());
		int[] downloadIndexes = generateIndexes(context, partitions.size());
		for (int i = 0; i < partitions.size(); i++) {
			Partition partition = partitions.get(i);
			NodeReduce<K, O, A> nodeReduce = new NodeReduce<>(reduceIndex, keyComparator);
			graph.addNode(partition, nodeReduce);

			int sortIndex = context.generateNodeIndex();
			int simpleReduceIndex = context.generateNodeIndex();
			for (int j = 0; j < sharders.size(); j++) {
				NodeShard<K, I> sharder = sharders.get(j);
				StreamId sharderOutput = sharder.newPartition();
				graph.addNodeStream(sharder, sharderOutput);
				StreamId reducerInput = sortReduceForward(context, sharderOutput, partition, sortIndex, simpleReduceIndex, uploadIndexes[i], downloadIndexes[j]);
				nodeReduce.addInput(reducerInput, accumulatorKeyFunction, reducer.accumulatorToOutput());
			}

			outputStreamIds.add(nodeReduce.getOutput());
		}

		return outputStreamIds;
	}

	private StreamId sortReduceForward(DataflowContext context, StreamId sourceStreamId, Partition targetPartition, int sortIndex, int simpleReduceIndex, int uploadIndex, int downloadIndex) {
		DataflowGraph graph = context.getGraph();
		Partition sourcePartition = graph.getPartition(sourceStreamId);

		NodeSort<K, I> nodeSort = new NodeSort<>(sortIndex, input.valueType(), inputKeyFunction, keyComparator, false, sortBufferSize, sourceStreamId);
		graph.addNode(sourcePartition, nodeSort);

		NodeReduceSimple<K, I, A, A> nodeReduce = new NodeReduceSimple<>(simpleReduceIndex, inputKeyFunction, keyComparator, reducer.inputToAccumulator());
		nodeReduce.addInput(nodeSort.getOutput());
		graph.addNode(sourcePartition, nodeReduce);

		return DatasetUtils.forwardChannel(context, accumulatorType, nodeReduce.getOutput(), targetPartition, uploadIndex, downloadIndex);
	}

	@Override
	public Collection<Dataset<?>> getBases() {
		return singletonList(input);
	}
}
