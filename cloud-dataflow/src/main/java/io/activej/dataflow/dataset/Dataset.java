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

import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.StreamId;

import java.util.List;

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

	public Dataset(Class<T> valueType) {
		this.valueType = valueType;
	}

	public final Class<T> valueType() {
		return valueType;
	}

	public abstract List<StreamId> channels(DataflowContext context);
}
