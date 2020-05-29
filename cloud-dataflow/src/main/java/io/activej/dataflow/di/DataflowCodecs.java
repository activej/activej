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

package io.activej.dataflow.di;

import io.activej.codec.StructuredCodec;
import io.activej.codec.StructuredOutput;
import io.activej.common.parse.ParseException;
import io.activej.dataflow.di.CodecsModule.SubtypeNameFactory;
import io.activej.dataflow.di.CodecsModule.Subtypes;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.node.*;
import io.activej.dataflow.server.command.DatagraphCommand;
import io.activej.dataflow.server.command.DatagraphCommandDownload;
import io.activej.dataflow.server.command.DatagraphCommandExecute;
import io.activej.dataflow.server.command.DatagraphResponse;
import io.activej.datastream.processor.StreamJoin.Joiner;
import io.activej.datastream.processor.StreamReducers.MergeSortReducer;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.datastream.processor.StreamReducers.ReducerToResult;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.AccumulatorToAccumulator;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.AccumulatorToOutput;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.InputToAccumulator;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.InputToOutput;
import io.activej.di.Key;
import io.activej.di.annotation.Provides;
import io.activej.di.module.AbstractModule;
import io.activej.di.module.Module;
import io.activej.di.util.Types;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.codec.StructuredCodec.ofObject;
import static io.activej.codec.StructuredCodecs.object;
import static io.activej.codec.StructuredCodecs.ofList;
import static io.activej.datastream.processor.StreamReducers.MergeDistinctReducer;

@SuppressWarnings({"rawtypes", "unchecked"})
public final class DataflowCodecs extends AbstractModule {

	private DataflowCodecs() {
	}

	public static Module create() {
		return new DataflowCodecs();
	}

	private static final Comparator<?> NATURAL_ORDER = Comparator.naturalOrder();
	private static final Class<?> NATURAL_ORDER_CLASS = NATURAL_ORDER.getClass();

	@Override
	protected void configure() {
		install(CodecsModule.create());

		bind(new Key<StructuredCodec<DatagraphCommand>>() {}).qualified(Subtypes.class);

		bind(Key.ofType(Types.parameterized(StructuredCodec.class, NATURAL_ORDER_CLASS)))
				.toInstance(StructuredCodec.ofObject(() -> NATURAL_ORDER));
	}

	@Provides
	StructuredCodec<StreamId> streamId() {
		return StructuredCodec.of(
				in -> new StreamId(in.readLong()),
				(out, item) -> out.writeLong(item.getId()));
	}

	@Provides
	StructuredCodec<InetSocketAddress> address() {
		return StructuredCodec.of(
				in -> {
					String str = in.readString();
					String[] split = str.split(":");
					if (split.length != 2) {
						throw new ParseException("Address should be split with a single ':'");
					}
					try {
						return new InetSocketAddress(InetAddress.getByName(split[0]), Integer.parseInt(split[1]));
					} catch (UnknownHostException e) {
						throw new ParseException(DataflowCodecs.class, "Failed to create InetSocketAddress", e);
					}
				},
				(out, addr) -> out.writeString(addr.getAddress().getHostAddress() + ':' + addr.getPort())
		);
	}

	@Provides
	StructuredCodec<DatagraphCommandDownload> datagraphCommandDownload(StructuredCodec<StreamId> streamId) {
		return object(DatagraphCommandDownload::new,
				"streamId", DatagraphCommandDownload::getStreamId, streamId);
	}

	@Provides
	StructuredCodec<DatagraphCommandExecute> datagraphCommandExecute(@Subtypes StructuredCodec<Node> node) {
		return object(DatagraphCommandExecute::new,
				"nodes", DatagraphCommandExecute::getNodes, ofList(node));
	}

	@Provides
	StructuredCodec<DatagraphResponse> datagraphResponse(StructuredCodec<String> string) {
		//noinspection ConstantConditions - intellji false positive
		return object(DatagraphResponse::new,
				"error", DatagraphResponse::getError, string.nullable());
	}

	@Provides
	StructuredCodec<NodeReduce.Input> nodeReduceInput(@Subtypes StructuredCodec<Reducer> reducer, @Subtypes StructuredCodec<Function> function) {
		return object(NodeReduce.Input::new,
				"reducer", NodeReduce.Input::getReducer, reducer,
				"keyFunction", NodeReduce.Input::getKeyFunction, function);
	}

	@Provides
	StructuredCodec<InputToAccumulator> inputToAccumulator(@Subtypes StructuredCodec<ReducerToResult> reducerToResult) {
		return object(InputToAccumulator::new,
				"reducerToResult", InputToAccumulator::getReducerToResult, reducerToResult);
	}

	@Provides
	StructuredCodec<InputToOutput> inputToOutput(@Subtypes StructuredCodec<ReducerToResult> reducerToResult) {
		return object(InputToOutput::new,
				"reducerToResult", InputToOutput::getReducerToResult, reducerToResult);
	}

	@Provides
	StructuredCodec<AccumulatorToAccumulator> accumulatorToAccumulator(@Subtypes StructuredCodec<ReducerToResult> reducerToResult) {
		return object(AccumulatorToAccumulator::new,
				"reducerToResult", AccumulatorToAccumulator::getReducerToResult, reducerToResult);
	}

	@Provides
	StructuredCodec<AccumulatorToOutput> accumulatorToOutput(@Subtypes StructuredCodec<ReducerToResult> reducerToResult) {
		return object(AccumulatorToOutput::new,
				"reducerToResult", AccumulatorToOutput::getReducerToResult, reducerToResult);
	}

	@Provides
	StructuredCodec<MergeDistinctReducer> mergeDistinctReducer() {
		return ofObject(MergeDistinctReducer::new);
	}

	@Provides
	StructuredCodec<MergeSortReducer> mergeSortReducer() {
		return ofObject(MergeSortReducer::new);
	}

	@Provides
	StructuredCodec<NodeDownload> nodeDownload(StructuredCodec<Class<?>> cls, StructuredCodec<InetSocketAddress> address, StructuredCodec<StreamId> streamId) {
		return object(NodeDownload::new,
				"type", NodeDownload::getType, cls,
				"address", NodeDownload::getAddress, address,
				"streamId", NodeDownload::getStreamId, streamId,
				"output", NodeDownload::getOutput, streamId);
	}

	@Provides
	StructuredCodec<NodeUpload> nodeUpload(StructuredCodec<Class<?>> cls, StructuredCodec<StreamId> streamId) {
		return object(NodeUpload::new,
				"type", NodeUpload::getType, cls,
				"streamId", NodeUpload::getStreamId, streamId);
	}

	@Provides
	StructuredCodec<NodeMap> nodeMap(@Subtypes StructuredCodec<Function> function, StructuredCodec<StreamId> streamId) {
		return object(NodeMap::new,
				"function", NodeMap::getFunction, function,
				"input", NodeMap::getInput, streamId,
				"output", NodeMap::getOutput, streamId);
	}

	@Provides
	StructuredCodec<NodeFilter> nodeFilter(@Subtypes StructuredCodec<Predicate> predicate, StructuredCodec<StreamId> streamId) {
		return object(NodeFilter::new,
				"predicate", NodeFilter::getPredicate, predicate,
				"input", NodeFilter::getInput, streamId,
				"output", NodeFilter::getOutput, streamId);
	}

	@Provides
	StructuredCodec<NodeShard> nodeShard(@Subtypes StructuredCodec<Function> function, StructuredCodec<StreamId> streamId, StructuredCodec<List<StreamId>> streamIds, StructuredCodec<Integer> integer) {
		return object(NodeShard::new,
				"keyFunction", NodeShard::getKeyFunction, function,
				"input", NodeShard::getInput, streamId,
				"outputs", NodeShard::getOutputs, streamIds,
				"nonce", NodeShard::getNonce, integer);
	}

	@Provides
	StructuredCodec<NodeMerge> nodeMerge(@Subtypes StructuredCodec<Function> function, @Subtypes StructuredCodec<Comparator> comparator, StructuredCodec<Boolean> bool, StructuredCodec<StreamId> streamId, StructuredCodec<List<StreamId>> streamIds) {
		return object(NodeMerge::new,
				"keyFunction", NodeMerge::getKeyFunction, function,
				"keyComparator", NodeMerge::getKeyComparator, comparator,
				"deduplicate", NodeMerge::isDeduplicate, bool,
				"inputs", NodeMerge::getInputs, streamIds,
				"output", NodeMerge::getOutput, streamId);
	}

	@Provides
	StructuredCodec<NodeReduce> nodeReduce(@Subtypes StructuredCodec<Comparator> comparator, StructuredCodec<StreamId> streamId, StructuredCodec<Map<StreamId, NodeReduce.Input>> inputs) {
		return object((a, b, c) -> new NodeReduce(a, b, c),
				"keyComparator", NodeReduce::getKeyComparator, comparator,
				"inputs", n -> (Map<StreamId, NodeReduce.Input>) n.getInputMap(), inputs,
				"output", NodeReduce::getOutput, streamId);
	}

	@Provides
	StructuredCodec<NodeReduceSimple> nodeReduceSimple(@Subtypes StructuredCodec<Function> function, @Subtypes StructuredCodec<Comparator> comparator, @Subtypes StructuredCodec<Reducer> reducer, StructuredCodec<StreamId> streamId, StructuredCodec<List<StreamId>> streamIds) {
		return object(NodeReduceSimple::new,
				"keyFunction", NodeReduceSimple::getKeyFunction, function,
				"keyComparator", NodeReduceSimple::getKeyComparator, comparator,
				"reducer", NodeReduceSimple::getReducer, reducer,
				"inputs", NodeReduceSimple::getInputs, streamIds,
				"output", NodeReduceSimple::getOutput, streamId);
	}

	@Provides
	StructuredCodec<NodeUnion> nodeUnion(StructuredCodec<StreamId> streamId, StructuredCodec<List<StreamId>> streamIds) {
		return object(NodeUnion::new,
				"inputs", NodeUnion::getInputs, streamIds,
				"output", NodeUnion::getOutput, streamId);
	}

	@Provides
	StructuredCodec<NodeSupplierOfId> nodeSupplierOfId(StructuredCodec<String> string, StructuredCodec<Integer> integer, StructuredCodec<StreamId> streamId) {
		return object(NodeSupplierOfId::new,
				"id", NodeSupplierOfId::getId, string,
				"partitionIndex", NodeSupplierOfId::getPartitionIndex, integer,
				"maxPartitions", NodeSupplierOfId::getMaxPartitions, integer,
				"output", NodeSupplierOfId::getOutput, streamId);
	}

	@Provides
	StructuredCodec<NodeConsumerOfId> nodeConsumerToList(StructuredCodec<String> string, StructuredCodec<Integer> integer, StructuredCodec<StreamId> streamId) {
		return object(NodeConsumerOfId::new,
				"id", NodeConsumerOfId::getId, string,
				"partitionIndex", NodeConsumerOfId::getPartitionIndex, integer,
				"maxPartitions", NodeConsumerOfId::getMaxPartitions, integer,
				"input", NodeConsumerOfId::getInput, streamId);
	}

	@Provides
	StructuredCodec<NodeSort> nodeSort(StructuredCodec<Class<?>> cls, @Subtypes StructuredCodec<Comparator> comparator, @Subtypes StructuredCodec<Function> function, StructuredCodec<StreamId> streamId, StructuredCodec<Boolean> bool, StructuredCodec<Integer> integer) {
		return ofObject(
				in -> new NodeSort(
						in.readKey("type", cls),
						in.readKey("keyFunction", function),
						in.readKey("keyComparator", comparator),
						in.readKey("deduplicate", bool),
						in.readKey("itemsInMemorySize", integer),
						in.readKey("input", streamId),
						in.readKey("output", streamId)),
				(StructuredOutput out, NodeSort node) -> {
					out.writeKey("type", cls, (Class<Object>) node.getType());
					out.writeKey("keyFunction", function, node.getKeyFunction());
					out.writeKey("keyComparator", comparator, node.getKeyComparator());
					out.writeKey("deduplicate", bool, node.isDeduplicate());
					out.writeKey("itemsInMemorySize", integer, node.getItemsInMemorySize());
					out.writeKey("input", streamId, node.getInput());
					out.writeKey("output", streamId, node.getOutput());
				});
	}

	@Provides
	StructuredCodec<NodeJoin> nodeJoin(@Subtypes StructuredCodec<Joiner> joiner, @Subtypes StructuredCodec<Comparator> comparator, @Subtypes StructuredCodec<Function> function, StructuredCodec<StreamId> streamId) {
		return ofObject(
				in -> new NodeJoin(
						in.readKey("left", streamId),
						in.readKey("right", streamId),
						in.readKey("output", streamId),
						in.readKey("keyComparator", comparator),
						in.readKey("leftKeyFunction", function),
						in.readKey("rightKeyFunction", function),
						in.readKey("joiner", joiner)),
				(StructuredOutput out, NodeJoin node) -> {
					out.writeKey("left", streamId, node.getLeft());
					out.writeKey("right", streamId, node.getRight());
					out.writeKey("output", streamId, node.getOutput());
					out.writeKey("keyComparator", comparator, node.getKeyComparator());
					out.writeKey("leftKeyFunction", function, node.getLeftKeyFunction());
					out.writeKey("rightKeyFunction", function, node.getRightKeyFunction());
					out.writeKey("joiner", joiner, node.getJoiner());
				});
	}

	@Provides
	SubtypeNameFactory subtypeNames() {
		return subtype -> {
			if (subtype == NATURAL_ORDER_CLASS) return "Comparator.naturalOrder";
			if (subtype == DatagraphCommandDownload.class) return "Download";
			if (subtype == DatagraphCommandExecute.class) return "Execute";
			if (subtype == DatagraphResponse.class) return "Response";
			return null;
		};
	}
}
