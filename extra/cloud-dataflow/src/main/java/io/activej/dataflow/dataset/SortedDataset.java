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

import io.activej.dataflow.graph.StreamSchema;

import java.util.Comparator;
import java.util.function.Function;

/**
 * Represents repartitioned and sorted dataset.
 * <p>Values with any specific key must reside in single partition, and each partition is sorted by key</p>
 *
 * @param <K> data item key
 * @param <T> data item type
 */
public abstract class SortedDataset<K, T> extends LocallySortedDataset<K, T> {
	protected SortedDataset(StreamSchema<T> streamSchema, Comparator<K> keyComparator, Class<K> keyType, Function<T, K> keyFunction) {
		super(streamSchema, keyComparator, keyType, keyFunction);
	}
}
