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

package io.activej.dataflow.dataset;

import io.activej.common.ref.RefInt;
import io.activej.common.tuple.Tuple2;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.StreamId;

import java.util.*;

import static java.util.Collections.emptyList;

/**
 * Represents distributed dataset which can span multiple partitions.
 * <p>
 * Typically it is implemented as result of some distributed operation:
 * <ul>
 * <li>Parallel query to some underlying distributed data source which is provided by 'environment'
 * <li>Transformation of some existing datasets
 * <li>Joins, unions, sorts of several datasets
 * <li>Repartitioned dataset downloaded from source dataset
 * </ul>
 * Internally this is achieved by creating a node on each server during compilation of Datagraph
 */
public abstract class Dataset<T> {
	private final Class<T> valueType;

	protected Dataset(Class<T> valueType) {
		this.valueType = valueType;
	}

	public final Class<T> valueType() {
		return valueType;
	}

	public abstract List<StreamId> channels(DataflowContext context);

	/**
	 * Returns a list of datasets that this dataset is based on.
	 * Can return empty list if this dataset is not based on any other dataset.
	 */
	public Collection<Dataset<?>> getBases() {
		return emptyList();
	}

	@Override
	public String toString() {
		String name = getClass().getSimpleName();
		return (name.startsWith("Dataset") ? name.substring(7) : name) + "<" + valueType.getSimpleName() + ">";
	}

	private static void writeDatasets(StringBuilder sb, Map<Dataset<?>, String> ids, Set<Tuple2<Dataset<?>, Dataset<?>>> visited, RefInt lastId, Dataset<?> dataset) {
		for (Dataset<?> base : dataset.getBases()) {
			if (!visited.add(new Tuple2<>(base, dataset))) {
				continue;
			}
			sb.append("  ")
					.append(ids.computeIfAbsent(base, $ -> "d" + lastId.value++))
					.append(" -> ")
					.append(ids.computeIfAbsent(dataset, $ -> "d" + lastId.value++))
					.append('\n');
			writeDatasets(sb, ids, visited, lastId, base);
		}
	}

	@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
	public final String toGraphViz() {
		StringBuilder sb = new StringBuilder("digraph {\n  node[shape=rect]\n\n");
		HashMap<Dataset<?>, String> ids = new HashMap<>();
		writeDatasets(sb, ids, new HashSet<>(), new RefInt(0), this);
		sb.append('\n');
		ids.forEach((dataset, id) -> sb.append("  " + id +
				" [label=" + '"' + dataset + '"' + "]\n"));
		return sb.append('}').toString();
	}
}
