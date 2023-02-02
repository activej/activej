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
import io.activej.dataflow.graph.*;
import io.activej.dataflow.node.Node;
import io.activej.dataflow.node.Nodes;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

@ExposedInternals
public final class Empty<T> extends Dataset<T> {
	public final @Nullable List<Partition> partitions;

	public Empty(StreamSchema<T> streamSchema, @Nullable List<Partition> partitions) {
		super(streamSchema);
		this.partitions = partitions;
	}

	@Override
	public List<StreamId> channels(DataflowContext context) {
		DataflowGraph graph = context.getGraph();
		List<StreamId> outputStreamIds = new ArrayList<>();
		List<Partition> partitions = this.partitions == null ? graph.getAvailablePartitions() : this.partitions;
		int index = context.generateNodeIndex();
		for (Partition partition : partitions) {
			Node node = Nodes.emptySupplier(index);
			graph.addNode(partition, node);
			outputStreamIds.addAll(node.getOutputs());
		}
		return outputStreamIds;
	}
}
