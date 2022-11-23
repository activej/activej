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

package io.activej.dataflow.calcite.dataset;

import io.activej.dataflow.calcite.node.FilterableNodeSupplierOfId;
import io.activej.dataflow.calcite.where.WherePredicate;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.graph.*;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

public final class DatasetSupplierOfPredicate<T> extends Dataset<T> {
	private final String id;
	private final WherePredicate predicate;
	private final @Nullable List<Partition> partitions;

	private DatasetSupplierOfPredicate(String id, WherePredicate predicate, StreamSchema<T> resultStreamSchema, @Nullable List<Partition> partitions) {
		super(resultStreamSchema);
		this.id = id;
		this.predicate = predicate;
		this.partitions = partitions;
	}

	public static <T> DatasetSupplierOfPredicate<T> create(String id, WherePredicate predicate, StreamSchema<T> resultStreamSchema, @Nullable List<Partition> partitions) {
		return new DatasetSupplierOfPredicate<>(id, predicate, resultStreamSchema, partitions);
	}

	public static <T> DatasetSupplierOfPredicate<T> create(String id, WherePredicate predicate, StreamSchema<T> resultStreamSchema) {
		return new DatasetSupplierOfPredicate<>(id, predicate, resultStreamSchema, null);
	}

	@Override
	public List<StreamId> channels(DataflowContext context) {
		DataflowGraph graph = context.getGraph();
		List<StreamId> outputStreamIds = new ArrayList<>();
		List<Partition> partitions = this.partitions == null ? graph.getAvailablePartitions() : this.partitions;
		int index = context.generateNodeIndex();
		for (int i = 0, size = partitions.size(); i < size; i++) {
			Partition partition = partitions.get(i);
			FilterableNodeSupplierOfId<T> node = new FilterableNodeSupplierOfId<>(index, id, predicate, i, size);
			graph.addNode(partition, node);
			outputStreamIds.add(node.getOutput());
		}
		return outputStreamIds;
	}
}
