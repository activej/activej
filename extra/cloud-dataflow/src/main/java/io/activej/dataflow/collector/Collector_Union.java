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

public final class Collector_Union<T> extends AbstractCollector<T, StreamUnion<T>, Collector_Union<T>> {

	private Collector_Union(Dataset<T> input, DataflowClient client) {
		super(input, client);
	}

	public static <T> Collector_Union<T> create(Dataset<T> input, DataflowClient client) {
		return new Collector_Union<>(input, client);
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
