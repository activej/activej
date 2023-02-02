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
import io.activej.dataflow.dataset.SortedDataset;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.node.Node;
import io.activej.dataflow.node.Nodes;
import io.activej.datastream.processor.StreamLeftJoin.LeftJoiner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static io.activej.dataflow.dataset.DatasetUtils.repartitionAndSort;

@ExposedInternals
public final class Join<K, L, R, V> extends SortedDataset<K, V> {
	public final SortedDataset<K, L> left;
	public final SortedDataset<K, R> right;
	public final LeftJoiner<K, L, R, V> leftJoiner;

	public final int sharderNonce = ThreadLocalRandom.current().nextInt();

	public Join(SortedDataset<K, L> left, SortedDataset<K, R> right, LeftJoiner<K, L, R, V> leftJoiner,
			StreamSchema<V> resultStreamSchema, Function<V, K> keyFunction) {
		super(resultStreamSchema, left.keyComparator(), left.keyType(), keyFunction);
		this.left = left;
		this.right = right;
		this.leftJoiner = leftJoiner;
	}

	@Override
	public List<StreamId> channels(DataflowContext context) {
		DataflowGraph graph = context.getGraph();
		List<StreamId> outputStreamIds = new ArrayList<>();

		DataflowContext next = context.withFixedNonce(sharderNonce);

		List<StreamId> leftStreamIds = left.channels(next);
		List<StreamId> rightStreamIds = repartitionAndSort(next, right, graph.getPartitions(leftStreamIds));

		assert leftStreamIds.size() == rightStreamIds.size();
		int index = context.generateNodeIndex();
		for (int i = 0; i < leftStreamIds.size(); i++) {
			StreamId leftStreamId = leftStreamIds.get(i);
			StreamId rightStreamId = rightStreamIds.get(i);
			Node node = Nodes.join(index, leftStreamId, rightStreamId, left.keyComparator(),
					left.keyFunction(), right.keyFunction(), leftJoiner);
			graph.addNode(graph.getPartition(leftStreamId), node);
			outputStreamIds.addAll(node.getOutputs());
		}
		return outputStreamIds;
	}

	@Override
	public Collection<Dataset<?>> getBases() {
		return List.of(left, right);
	}
}
