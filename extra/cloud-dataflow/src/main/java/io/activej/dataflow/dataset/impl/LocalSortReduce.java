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

import io.activej.common.annotation.ExposedInternals;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.LocallySortedDataset;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.node.impl.ReduceSimple;
import io.activej.datastream.processor.reducer.Reducer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

@ExposedInternals
public final class LocalSortReduce<K, I, O> extends LocallySortedDataset<K, O> {
	public final LocallySortedDataset<K, I> input;
	public final Reducer<K, I, O, ?> reducer;

	public LocalSortReduce(
		LocallySortedDataset<K, I> input, Reducer<K, I, O, ?> reducer, StreamSchema<O> resultStreamSchema,
		Function<O, K> resultKeyFunction
	) {
		super(resultStreamSchema, input.keyComparator(), input.keyType(), resultKeyFunction);
		this.input = input;
		this.reducer = reducer;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<StreamId> channels(DataflowContext context) {
		DataflowGraph graph = context.getGraph();
		List<StreamId> outputStreamIds = new ArrayList<>();
		int index = context.generateNodeIndex();
		for (StreamId streamId : input.channels(context)) {
			ReduceSimple<K, I, O, Object>.Builder nodeBuilder = ReduceSimple.builder(index, input.keyFunction(),
				input.keyComparator(), (Reducer<K, I, O, Object>) reducer);
			nodeBuilder.withInput(streamId);
			ReduceSimple<K, I, O, Object> node = nodeBuilder.build();
			graph.addNode(graph.getPartition(streamId), node);
			outputStreamIds.add(node.output);
		}
		return outputStreamIds;
	}

	@Override
	public Collection<Dataset<?>> getBases() {
		return List.of(input);
	}
}
