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
import io.activej.reactor.Reactor;

import java.util.Comparator;
import java.util.function.Function;

import static io.activej.datastream.processor.reducer.Reducers.deduplicateReducer;
import static io.activej.datastream.processor.reducer.Reducers.mergeReducer;

public final class MergeCollector<K, T> extends AbstractCollector<T, StreamReducer<K, T, Void>> {
	private final Function<T, K> keyFunction;
	private final Comparator<K> keyComparator;
	private boolean deduplicate;

	private MergeCollector(Reactor reactor, Dataset<T> input, DataflowClient client,
			Function<T, K> keyFunction, Comparator<K> keyComparator) {
		super(reactor, input, client);
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
	}

	public static <K, T> MergeCollector<K, T> create(Reactor reactor, Dataset<T> input, DataflowClient client,
			Function<T, K> keyFunction, Comparator<K> keyComparator) {
		return builder(reactor, input, client, keyFunction, keyComparator).build();
	}

	public static <K, T> MergeCollector<K, T> create(Reactor reactor, LocallySortedDataset<K, T> input, DataflowClient client) {
		return builder(reactor, input, client).build();
	}

	public static <K, T> MergeCollector<K, T>.Builder builder(Reactor reactor, Dataset<T> input, DataflowClient client,
			Function<T, K> keyFunction, Comparator<K> keyComparator) {
		return new MergeCollector<>(reactor, input, client, keyFunction, keyComparator).new Builder();
	}

	public static <K, T> MergeCollector<K, T>.Builder builder(Reactor reactor, LocallySortedDataset<K, T> input, DataflowClient client) {
		return new MergeCollector<>(reactor, input, client, input.keyFunction(), input.keyComparator()).new Builder();
	}

	public final class Builder extends AbstractCollector<T, StreamReducer<K, T, Void>>.Builder<Builder, MergeCollector<K, T>> {
		private Builder() {}

		public Builder withDeduplicate() {
			checkNotBuilt(this);
			MergeCollector.this.deduplicate = true;
			return this;
		}

		public Builder withDeduplicate(boolean deduplicate) {
			checkNotBuilt(this);
			MergeCollector.this.deduplicate = deduplicate;
			return this;
		}
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
