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
import io.activej.dataflow.node.*;
import io.activej.datastream.processor.StreamLimiter;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.datastream.processor.StreamSkip;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiFunction;
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
		List<Node_Shard<K, I>> sharders = new ArrayList<>();
		int sharderIndex = context.generateNodeIndex();
		for (StreamId inputStreamId : inputs) {
			Partition partition = graph.getPartition(inputStreamId);
			Node_Shard<K, I> sharder = new Node_Shard<>(sharderIndex, keyFunction, inputStreamId, nonce);
			graph.addNode(partition, sharder);
			sharders.add(sharder);
		}

		int reducerIndex = context.generateNodeIndex();
		int[] downloadIndexes = generateIndexes(context, sharders.size());
		int[] uploadIndexes = generateIndexes(context, partitions.size());
		for (int i = 0; i < partitions.size(); i++) {
			Partition partition = partitions.get(i);
			Node_Reduce<K, O, A> nodeReduce = new Node_Reduce<>(reducerIndex, keyComparator);
			graph.addNode(partition, nodeReduce);

			for (int j = 0; j < sharders.size(); j++) {
				Node_Shard<K, I> sharder = sharders.get(j);
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

	public static <T, K> List<StreamId> repartition(DataflowContext context,
			List<StreamId> inputs,
			StreamSchema<T> inputStreamSchema,
			Function<T, K> keyFunction,
			List<Partition> partitions) {

		DataflowGraph graph = context.getGraph();
		int nonce = context.getNonce();
		List<StreamId> outputStreamIds = new ArrayList<>();

		List<Node_Shard<K, T>> sharders = new ArrayList<>();
		int shardIndex = context.generateNodeIndex();
		for (StreamId inputStreamId : inputs) {
			Partition partition = graph.getPartition(inputStreamId);
			Node_Shard<K, T> sharder = new Node_Shard<>(shardIndex, keyFunction, inputStreamId, nonce);
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
				Node_Shard<K, T> sharder = sharders.get(j);
				StreamId sharderOutput = sharder.newPartition();
				graph.addNodeStream(sharder, sharderOutput);
				StreamId unionInput = forwardChannel(context, inputStreamSchema, sharderOutput, partition, uploadIndexes[i], downloadIndexes[j]);
				unionInputs.add(unionInput);
			}
			Node_Union<T> nodeUnion = new Node_Union<>(unionIndex, unionInputs);
			graph.addNode(partition, nodeUnion);

			outputStreamIds.add(nodeUnion.getOutput());
		}

		return outputStreamIds;
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
		Node_Upload<T> nodeUpload = new Node_Upload<>(uploadIndex, streamSchema, sourceStreamId);
		Node_Download<T> nodeDownload = new Node_Download<>(downloadIndex, streamSchema, sourcePartition.getAddress(), sourceStreamId);
		graph.addNode(sourcePartition, nodeUpload);
		graph.addNode(targetPartition, nodeDownload);
		return nodeDownload.getOutput();
	}

	public static int[] generateIndexes(DataflowContext context, int size) {
		return IntStream.generate(context::generateNodeIndex).limit(size).toArray();
	}

	public static StreamId limitStream(DataflowGraph graph, int index, long limit, StreamId streamId) {
		Node_OffsetLimit<?> node = new Node_OffsetLimit<>(index, 0, limit, streamId);
		graph.addNode(graph.getPartition(streamId), node);
		return node.getOutput();
	}

	public static List<StreamId> offsetLimit(DataflowContext context,
			List<StreamId> inputs,
			long offset,
			long limit,
			BiFunction<List<StreamId>, Partition, StreamId> inputReducer
	) {

		if (offset == StreamSkip.NO_SKIP && limit == StreamLimiter.NO_LIMIT) return inputs;

		DataflowGraph graph = context.getGraph();

		if (inputs.isEmpty()) return inputs;

		if (inputs.size() == 1) {
			return toOutput(graph, context.generateNodeIndex(), inputs.get(0), offset, limit);
		}

		if (limit != StreamLimiter.NO_LIMIT) {
			List<StreamId> newStreamIds = new ArrayList<>(inputs.size());
			for (StreamId streamId : inputs) {
				StreamId limitedStream = limitStream(graph, context.generateNodeIndex(), offset + limit, streamId);
				newStreamIds.add(limitedStream);
			}
			inputs = newStreamIds;
		}

		StreamId randomStreamId = inputs.get(Math.abs(context.getNonce()) % inputs.size());
		Partition randomPartition = graph.getPartition(randomStreamId);

		StreamId newStreamId = inputReducer.apply(inputs, randomPartition);

		return toOutput(graph, context.generateNodeIndex(), newStreamId, offset, limit);
	}

	private static <T> List<StreamId> toOutput(DataflowGraph graph, int index, StreamId streamId, long offset, long limit) {
		Node_OffsetLimit<T> node = new Node_OffsetLimit<>(index, offset, limit, streamId);
		graph.addNode(graph.getPartition(streamId), node);
		return List.of(node.getOutput());
	}

}
