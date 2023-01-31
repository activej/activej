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
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamUnion;
import io.activej.reactor.Reactor;

public final class UnionCollector<T> extends AbstractCollector<T, StreamUnion<T>> {

	private UnionCollector(Reactor reactor, Dataset<T> input, DataflowClient client) {
		super(reactor, input, client);
	}

	public static <T> UnionCollector<T> create(Reactor reactor, Dataset<T> input, DataflowClient client) {
		return builder(reactor, input, client).build();
	}

	public static <T> UnionCollector<T>.Builder builder(Reactor reactor, Dataset<T> input, DataflowClient client) {
		return new UnionCollector<>(reactor, input, client).new Builder();
	}

	public final class Builder extends AbstractCollector<T, StreamUnion<T>>.Builder<Builder, UnionCollector<T>> {
		private Builder() {}
	}

	@Override
	protected StreamUnion<T> createAccumulator() {
		return StreamUnion.create();
	}

	@Override
	protected void accumulate(StreamUnion<T> accumulator, StreamSupplier<T> supplier) {
		supplier.streamTo(accumulator.newInput());
	}

	@Override
	protected StreamSupplier<T> getResult(StreamUnion<T> accumulator) {
		return accumulator.getOutput();
	}
}
