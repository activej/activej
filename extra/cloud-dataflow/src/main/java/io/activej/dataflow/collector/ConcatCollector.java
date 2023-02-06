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
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.reactor.Reactor;

import java.util.ArrayList;
import java.util.List;

public final class ConcatCollector<T> extends AbstractCollector<T, List<StreamSupplier<T>>> {

	private ConcatCollector(Reactor reactor, Dataset<T> input, DataflowClient client) {
		super(reactor, input, client);
	}

	public static <T> ConcatCollector<T> create(Reactor reactor, Dataset<T> input, DataflowClient client) {
		return builder(reactor, input, client).build();
	}

	public static <T> ConcatCollector<T>.Builder builder(Reactor reactor, Dataset<T> input, DataflowClient client) {
		return new ConcatCollector<>(reactor, input, client).new Builder();
	}

	public final class Builder extends AbstractCollector<T, List<StreamSupplier<T>>>.Builder<Builder, ConcatCollector<T>> {
		private Builder() {}
	}

	@Override
	protected List<StreamSupplier<T>> createAccumulator() {
		return new ArrayList<>();
	}

	@Override
	protected void accumulate(List<StreamSupplier<T>> accumulator, StreamSupplier<T> supplier) {
		accumulator.add(supplier);
	}

	@Override
	protected StreamSupplier<T> getResult(List<StreamSupplier<T>> accumulator) {
		return StreamSuppliers.concat(accumulator)
				.withEndOfStream(eos -> eos
						.whenException(e -> accumulator.forEach(supplier -> supplier.closeEx(e))));
	}
}
