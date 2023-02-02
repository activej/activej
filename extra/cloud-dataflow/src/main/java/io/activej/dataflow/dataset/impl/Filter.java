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
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.node.Node;
import io.activej.dataflow.node.Nodes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

@ExposedInternals
public final class Filter<T> extends Dataset<T> {
	public final Dataset<T> input;
	public final Predicate<T> predicate;

	public Filter(Dataset<T> input, Predicate<T> predicate) {
		super(input.streamSchema());
		this.input = input;
		this.predicate = predicate;
	}

	@Override
	public List<StreamId> channels(DataflowContext context) {
		DataflowGraph graph = context.getGraph();
		List<StreamId> outputStreamIds = new ArrayList<>();
		List<StreamId> streamIds = input.channels(context);
		int index = context.generateNodeIndex();
		for (StreamId streamId : streamIds) {
			Node node = Nodes.filter(index, predicate, streamId);
			graph.addNode(graph.getPartition(streamId), node);
			outputStreamIds.addAll(node.getOutputs());
		}
		return outputStreamIds;
	}

	@Override
	public Collection<Dataset<?>> getBases() {
		return List.of(input);
	}
}
