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

import java.util.ArrayList;
import java.util.List;

public final class Collector_Concat<T> extends AbstractCollector<T, List<StreamSupplier<T>>, Collector_Concat<T>> {

	private Collector_Concat(Dataset<T> input, DataflowClient client) {
		super(input, client);
	}

	public static <T> Collector_Concat<T> create(Dataset<T> input, DataflowClient client) {
		return new Collector_Concat<>(input, client);
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
		return StreamSupplier.concat(accumulator)
				.withEndOfStream(eos -> eos
						.whenException(e -> accumulator.forEach(supplier -> supplier.closeEx(e))));
	}
}
