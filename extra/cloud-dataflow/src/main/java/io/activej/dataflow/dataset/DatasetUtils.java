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

package io.activej.dataflow.dataset;

import io.activej.dataflow.graph.*;
import io.activej.dataflow.node.NodeDownload;
import io.activej.dataflow.node.NodeReduce;
import io.activej.dataflow.node.NodeShard;
import io.activej.dataflow.node.NodeUpload;
import io.activej.datastream.processor.StreamReducers.Reducer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.activej.datastream.processor.StreamReducers.mergeReducer;

public class DatasetUtils {

	public static <K, I, O, A> List<StreamId> repartitionAndReduce(DataflowContext context,
			List<StreamId> inputs,
			StreamSchema<I> inputStreamSchema,
			Function<I, K> keyFunction,
			Comparator<K> keyComparator,
			Reducer<K, I, O, A> reducer,
			List<Partition> partitions) {
		DataflowGraph graph = context.getGraph();
		int nonce = context.getNonce();
		List<StreamId> outputStreamIds = new ArrayList<>();
		List<NodeShard<K, I>> sharders = new ArrayList<>();
		int sharderIndex = context.generateNodeIndex();
		for (StreamId inputStreamId : inputs) {
			Partition partition = graph.getPartition(inputStreamId);
			NodeShard<K, I> sharder = new NodeShard<>(sharderIndex, keyFunction, inputStreamId, nonce);
			graph.addNode(partition, sharder);
			sharders.add(sharder);
		}

		int reducerIndex = context.generateNodeIndex();
		int[] downloadIndexes = generateIndexes(context, sharders.size());
		int[] uploadIndexes = generateIndexes(context, partitions.size());
		for (int i = 0; i < partitions.size(); i++) {
			Partition partition = partitions.get(i);
			NodeReduce<K, O, A> nodeReduce = new NodeReduce<>(reducerIndex, keyComparator);
			graph.addNode(partition, nodeReduce);

			for (int j = 0; j < sharders.size(); j++) {
				NodeShard<K, I> sharder = sharders.get(j);
				StreamId sharderOutput = sharder.newPartition();
				graph.addNodeStream(sharder, sharderOutput);
				StreamId reducerInput = forwardChannel(context, inputStreamSchema, sharderOutput, partition, uploadIndexes[i], downloadIndexes[j]);
				nodeReduce.addInput(reducerInput, keyFunction, reducer);
			}

			outputStreamIds.add(nodeReduce.getOutput());
		}

		return outputStreamIds;
	}

	public static <K, I, O, A> List<StreamId> repartitionAndReduce(DataflowContext context,
			LocallySortedDataset<K, I> input,
			Reducer<K, I, O, A> reducer,
			List<Partition> partitions) {
		return repartitionAndReduce(context, input.channels(context.withoutFixedNonce()), input.streamSchema(), input.keyFunction(), input.keyComparator(), reducer, partitions);
	}

	public static <K, T> List<StreamId> repartitionAndSort(DataflowContext context, LocallySortedDataset<K, T> input,
			List<Partition> partitions) {
		return repartitionAndReduce(context, input, mergeReducer(), partitions);
	}

	public static <T> StreamId forwardChannel(DataflowContext context, StreamSchema<T> streamSchema,
			StreamId sourceStreamId, Partition targetPartition, int uploadIndex, int downloadIndex) {
		Partition sourcePartition = context.getGraph().getPartition(sourceStreamId);
		return forwardChannel(context, streamSchema, sourcePartition, targetPartition, sourceStreamId, uploadIndex, downloadIndex);
	}

	private static <T> StreamId forwardChannel(DataflowContext context, StreamSchema<T> streamSchema,
			Partition sourcePartition, Partition targetPartition,
			StreamId sourceStreamId, int uploadIndex, int downloadIndex) {
		if (sourcePartition == targetPartition) {
			return sourceStreamId;
		}
		DataflowGraph graph = context.getGraph();
		NodeUpload<T> nodeUpload = new NodeUpload<>(uploadIndex, streamSchema, sourceStreamId);
		NodeDownload<T> nodeDownload = new NodeDownload<>(downloadIndex, streamSchema, sourcePartition.getAddress(), sourceStreamId);
		graph.addNode(sourcePartition, nodeUpload);
		graph.addNode(targetPartition, nodeDownload);
		return nodeDownload.getOutput();
	}

	public static int[] generateIndexes(DataflowContext context, int size) {
		return IntStream.generate(context::generateNodeIndex).limit(size).toArray();
	}
}
