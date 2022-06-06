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
import io.activej.dataflow.node.NodeSupplierOfId;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

public final class DatasetSupplierOfId<T> extends Dataset<T> {
	private final String id;
	private final @Nullable List<Partition> partitions;

	public DatasetSupplierOfId(String id, Class<T> resultType, @Nullable List<Partition> partitions) {
		super(resultType);
		this.id = id;
		this.partitions = partitions;
	}

	@Override
	public List<StreamId> channels(DataflowContext context) {
		DataflowGraph graph = context.getGraph();
		List<StreamId> outputStreamIds = new ArrayList<>();
		List<Partition> partitions = this.partitions == null ? graph.getAvailablePartitions() : this.partitions;
		int index = context.generateNodeIndex();
		for (int i = 0, size = partitions.size(); i < size; i++) {
			Partition partition = partitions.get(i);
			NodeSupplierOfId<T> node = new NodeSupplierOfId<>(index, id, i, size);
			graph.addNode(partition, node);
			outputStreamIds.add(node.getOutput());
		}
		return outputStreamIds;
	}
}
