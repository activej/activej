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

import java.util.Comparator;
import java.util.function.Function;

/**
 * Represents a locally sorted dataset, which means that data on each stream is sorted by key independently.
 * <p>However, values with any specific key may belong to multiple partitions.
 * In other words, there may be duplicates of the same key in different partitions.</p>
 *
 * @param <K> data item key
 * @param <T> data item type
 */
public abstract class LocallySortedDataset<K, T> extends Dataset<T> {
	private final Comparator<K> keyComparator;

	private final Class<K> keyType;

	private final Function<T, K> keyFunction;

	protected LocallySortedDataset(Class<T> valueType, Comparator<K> keyComparator, Class<K> keyType,
			Function<T, K> keyFunction) {
		super(valueType);
		this.keyComparator = keyComparator;
		this.keyType = keyType;
		this.keyFunction = keyFunction;
	}

	public final Comparator<K> keyComparator() {
		return keyComparator;
	}

	public final Class<K> keyType() {
		return keyType;
	}

	public final Function<T, K> keyFunction() {
		return keyFunction;
	}
}
