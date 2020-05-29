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

import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.node.NodeDownload;
import io.activej.dataflow.node.NodeReduce;
import io.activej.dataflow.node.NodeShard;
import io.activej.dataflow.node.NodeUpload;
import io.activej.datastream.processor.StreamReducers;
import io.activej.datastream.processor.StreamReducers.Reducer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class DatasetUtils {

	public static <K, I, O, A> List<StreamId> repartitionAndReduce(DataflowContext context,
	                                                               LocallySortedDataset<K, I> input,
	                                                               Reducer<K, I, O, A> reducer,
	                                                               List<Partition> partitions) {
		DataflowGraph graph = context.getGraph();
		int nonce = context.getNonce();
		Function<I, K> keyFunction = input.keyFunction();
		List<StreamId> outputStreamIds = new ArrayList<>();
		List<NodeShard<K, I>> sharders = new ArrayList<>();
		int sharderIndex = context.generateNodeIndex();
		for (StreamId inputStreamId : input.channels(context.withoutFixedNonce())) {
			Partition partition = graph.getPartition(inputStreamId);
			NodeShard<K, I> sharder = new NodeShard<>(sharderIndex, keyFunction, inputStreamId, nonce);
			graph.addNode(partition, sharder);
			sharders.add(sharder);
		}

		int reducerIndex = context.generateNodeIndex();
		for (Partition partition : partitions) {
			NodeReduce<K, O, A> nodeReduce = new NodeReduce<>(reducerIndex, input.keyComparator());
			graph.addNode(partition, nodeReduce);

			for (NodeShard<K, I> sharder : sharders) {
				StreamId sharderOutput = sharder.newPartition();
				graph.addNodeStream(sharder, sharderOutput);
				StreamId reducerInput = forwardChannel(context, input.valueType(), sharderOutput, partition);
				nodeReduce.addInput(reducerInput, keyFunction, reducer);
			}

			outputStreamIds.add(nodeReduce.getOutput());
		}

		return outputStreamIds;
	}

	public static <K, T> List<StreamId> repartitionAndSort(DataflowContext context, LocallySortedDataset<K, T> input,
	                                                       List<Partition> partitions) {
		return repartitionAndReduce(context, input, StreamReducers.mergeSortReducer(), partitions);
	}

	public static <T> StreamId forwardChannel(DataflowContext context, Class<T> type,
	                                          StreamId sourceStreamId, Partition targetPartition) {
		Partition sourcePartition = context.getGraph().getPartition(sourceStreamId);
		return forwardChannel(context, type, sourcePartition, targetPartition, sourceStreamId);
	}

	private static <T> StreamId forwardChannel(DataflowContext context, Class<T> type,
	                                           Partition sourcePartition, Partition targetPartition,
	                                           StreamId sourceStreamId) {
//		if (sourcePartition == targetPartition) {
//			return sourceStreamId;
//		}
		DataflowGraph graph = context.getGraph();
		NodeUpload<T> nodeUpload = new NodeUpload<>(context.generateNodeIndex(), type, sourceStreamId);
		NodeDownload<T> nodeDownload = new NodeDownload<>(context.generateNodeIndex(), type, sourcePartition.getAddress(), sourceStreamId);
		graph.addNode(sourcePartition, nodeUpload);
		graph.addNode(targetPartition, nodeDownload);
		return nodeDownload.getOutput();
	}
}
