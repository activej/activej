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
import io.activej.dataflow.dataset.LocallySortedDataset;
import io.activej.dataflow.dataset.SortedDataset;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.StreamId;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;

import static io.activej.dataflow.dataset.DatasetUtils.repartitionAndSort;

public final class DatasetRepartitionAndSort<K, T> extends SortedDataset<K, T> {
	private final LocallySortedDataset<K, T> input;
	private final @Nullable List<Partition> partitions;

	public DatasetRepartitionAndSort(LocallySortedDataset<K, T> input) {
		this(input, null);
	}

	public DatasetRepartitionAndSort(LocallySortedDataset<K, T> input, @Nullable List<Partition> partitions) {
		super(input.streamSchema(), input.keyComparator(), input.keyType(), input.keyFunction());
		this.input = input;
		this.partitions = partitions;
	}

	@Override
	public List<StreamId> channels(DataflowContext context) {
		List<Partition> ps = partitions != null && !partitions.isEmpty() ?
				partitions :
				context.getGraph().getAvailablePartitions();
		return repartitionAndSort(context, input, ps);
	}

	@Override
	public Collection<Dataset<?>> getBases() {
		return List.of(input);
	}
}
