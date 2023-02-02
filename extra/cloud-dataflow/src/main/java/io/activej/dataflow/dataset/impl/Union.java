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
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.node.impl.Merge;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.activej.dataflow.dataset.DatasetUtils.repartitionAndSort;

@ExposedInternals
public final class Union<K, T> extends SortedDataset<K, T> {
	public final SortedDataset<K, T> left;
	public final SortedDataset<K, T> right;

	public final int sharderNonce;

	public Union(SortedDataset<K, T> left, SortedDataset<K, T> right, int nonce) {
		super(left.streamSchema(), left.keyComparator(), left.keyType(), left.keyFunction());
		this.left = left;
		this.right = right;
		this.sharderNonce = nonce;
	}

	@Override
	public List<StreamId> channels(DataflowContext context) {
		DataflowGraph graph = context.getGraph();
		List<StreamId> outputStreamIds = new ArrayList<>();

		DataflowContext next = context.withFixedNonce(sharderNonce);

		List<StreamId> leftStreamIds = left.channels(next);
		List<StreamId> rightStreamIds = repartitionAndSort(next, right, graph.getPartitions(leftStreamIds));

		Map<Partition, List<StreamId>> partitioned = Stream.concat(leftStreamIds.stream(), rightStreamIds.stream())
				.collect(Collectors.groupingBy(graph::getPartition));

		int index = context.generateNodeIndex();

		for (Map.Entry<Partition, List<StreamId>> entry : partitioned.entrySet()) {
			List<StreamId> streamIds = entry.getValue();

			Merge<K, T>.Builder nodeMergeBuilder = Merge.builder(index, keyFunction(), keyComparator(), true);
			for (StreamId streamId : streamIds) {
				nodeMergeBuilder.withInput(streamId);
			}
			Merge<K, T> nodeMerge = nodeMergeBuilder.build();
			graph.addNode(entry.getKey(), nodeMerge);

			outputStreamIds.add(nodeMerge.output);
		}

		return outputStreamIds;
	}

	@Override
	public Collection<Dataset<?>> getBases() {
		return List.of(left, right);
	}
}
