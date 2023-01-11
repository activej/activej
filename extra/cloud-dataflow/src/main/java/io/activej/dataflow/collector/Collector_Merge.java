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

package io.activej.dataflow.collector;

import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.LocallySortedDataset;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamReducer;

import java.util.Comparator;
import java.util.function.Function;

import static io.activej.datastream.processor.StreamReducers.deduplicateReducer;
import static io.activej.datastream.processor.StreamReducers.mergeReducer;

public final class Collector_Merge<K, T> extends AbstractCollector<T, StreamReducer<K, T, Void>, Collector_Merge<K, T>> {
	private final Function<T, K> keyFunction;
	private final Comparator<K> keyComparator;
	private final boolean deduplicate;

	private Collector_Merge(Dataset<T> input, DataflowClient client,
			Function<T, K> keyFunction, Comparator<K> keyComparator, boolean deduplicate) {
		super(input, client);
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.deduplicate = deduplicate;
	}

	public static <K, T> Collector_Merge<K, T> create(Dataset<T> input, DataflowClient client,
			Function<T, K> keyFunction, Comparator<K> keyComparator, boolean deduplicate) {
		return new Collector_Merge<>(input, client, keyFunction, keyComparator, deduplicate);
	}

	public static <K, T> Collector_Merge<K, T> create(LocallySortedDataset<K, T> input, DataflowClient client, boolean deduplicate) {
		return new Collector_Merge<>(input, client, input.keyFunction(), input.keyComparator(), deduplicate);
	}

	@Override
	protected StreamReducer<K, T, Void> createAccumulator() {
		return StreamReducer.create(keyComparator);
	}

	@Override
	protected void accumulate(StreamReducer<K, T, Void> accumulator, StreamSupplier<T> supplier) {
		supplier.streamTo(accumulator.newInput(keyFunction, deduplicate ? deduplicateReducer() : mergeReducer()));
	}

	@Override
	protected StreamSupplier<T> getResult(StreamReducer<K, T, Void> accumulator) {
		return accumulator.getOutput();
	}
}
