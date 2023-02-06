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

package io.activej.datastream.supplier;

import io.activej.common.recycle.Recyclers;
import io.activej.common.tuple.Tuple2;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.StreamConsumerWithResult;
import io.activej.promise.Promise;
import io.activej.promise.Promises;

import java.util.function.Function;

/**
 * A {@link StreamSupplier} that is bound with some {@link Promise}
 * that represents some kind of result from the streaming process.
 */
public final class StreamSupplierWithResult<T, X> {
	static {
		Recyclers.register(StreamSupplierWithResult.class, item -> {
			Recyclers.recycle(item.supplier);
			Recyclers.recycle(item.result);
		});
	}

	private final StreamSupplier<T> supplier;
	private final Promise<X> result;

	private StreamSupplierWithResult(StreamSupplier<T> supplier, Promise<X> result) {
		this.supplier = supplier;
		this.result = result;
	}

	public static <T, X> StreamSupplierWithResult<T, X> of(StreamSupplier<T> supplier, Promise<X> result) {
		return new StreamSupplierWithResult<>(supplier, result);
	}

	public Promise<X> streamTo(StreamConsumer<T> consumer) {
		return supplier.streamTo(consumer)
				.then($ -> result);
	}

	public <Y> Promise<Tuple2<X, Y>> streamTo(StreamConsumerWithResult<T, Y> consumer) {
		return supplier.streamTo(consumer.getConsumer())
				.then(() -> Promises.toTuple(result, consumer.getResult()));
	}

	public StreamSupplierWithResult<T, X> sanitize() {
		return new StreamSupplierWithResult<>(supplier,
				supplier.getEndOfStream().combine(result.whenException(supplier::closeEx), ($, v) -> v).async());
	}

	public <T1, X1> StreamSupplierWithResult<T1, X1> transform(
			Function<StreamSupplier<T>, StreamSupplier<T1>> consumerTransformer,
			Function<Promise<X>, Promise<X1>> resultTransformer) {
		return new StreamSupplierWithResult<>(
				consumerTransformer.apply(supplier),
				resultTransformer.apply(result));
	}

	public <T1> StreamSupplierWithResult<T1, X> transformSupplier(Function<StreamSupplier<T>, StreamSupplier<T1>> consumerTransformer) {
		return transform(consumerTransformer, Function.identity());
	}

	public <X1> StreamSupplierWithResult<T, X1> transformResult(Function<Promise<X>, Promise<X1>> resultTransformer) {
		return transform(Function.identity(), resultTransformer);
	}

	public static <T, X> StreamSupplierWithResult<T, X> ofPromise(Promise<StreamSupplierWithResult<T, X>> promise) {
		if (promise.isResult()) return promise.getResult();
		return of(
				StreamSuppliers.ofPromise(promise.map(StreamSupplierWithResult::getSupplier)),
				promise.then(StreamSupplierWithResult::getResult));
	}

	public StreamSupplier<T> getSupplier() {
		return supplier;
	}

	public Promise<X> getResult() {
		return result;
	}
}
