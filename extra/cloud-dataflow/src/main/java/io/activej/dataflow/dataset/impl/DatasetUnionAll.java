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
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.node.NodeUnion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class DatasetUnionAll<T> extends Dataset<T> {
	private final Dataset<T> left;
	private final Dataset<T> right;

	public DatasetUnionAll(Dataset<T> left, Dataset<T> right) {
		super(left.valueType());
		this.left = left;
		this.right = right;
	}

	@Override
	public List<StreamId> channels(DataflowContext context) {
		DataflowGraph graph = context.getGraph();
		List<StreamId> outputStreamIds = new ArrayList<>();

		List<StreamId> leftStreamIds = left.channels(context);
		List<StreamId> rightStreamIds = right.channels(context);

		Map<Partition, List<StreamId>> partitioned = Stream.concat(leftStreamIds.stream(), rightStreamIds.stream())
				.collect(Collectors.groupingBy(graph::getPartition));

		int index = context.generateNodeIndex();

		for (Map.Entry<Partition, List<StreamId>> entry : partitioned.entrySet()) {
			List<StreamId> streamIds = entry.getValue();
			assert !streamIds.isEmpty();

			if (streamIds.size() == 1) {
				outputStreamIds.add(streamIds.get(0));
				continue;
			}

			NodeUnion<T> nodeUnion = new NodeUnion<>(index, streamIds);
			graph.addNode(entry.getKey(), nodeUnion);

			outputStreamIds.add(nodeUnion.getOutput());
		}

		return outputStreamIds;
	}

	@Override
	public Collection<Dataset<?>> getBases() {
		return List.of(left, right);
	}
}
