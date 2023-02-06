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

package io.activej.datastream.consumer;

import io.activej.common.recycle.Recyclers;
import io.activej.promise.Promise;

import java.util.function.Function;

/**
 * A {@link StreamConsumer} that is bound with some {@link Promise}
 * that represents some kind of result from the streaming process.
 */
public final class StreamConsumerWithResult<T, X> {
	static {
		Recyclers.register(StreamConsumerWithResult.class, item -> {
			Recyclers.recycle(item.consumer);
			Recyclers.recycle(item.result);
		});
	}

	private final StreamConsumer<T> consumer;

	private final Promise<X> result;

	private StreamConsumerWithResult(StreamConsumer<T> consumer, Promise<X> result) {
		this.consumer = consumer;
		this.result = result;
	}

	public static <T, X> StreamConsumerWithResult<T, X> of(StreamConsumer<T> consumer, Promise<X> result) {
		return new StreamConsumerWithResult<>(consumer, result);
	}

	public StreamConsumerWithResult<T, X> sanitize() {
		return new StreamConsumerWithResult<>(consumer,
				consumer.getAcknowledgement().combine(result.whenException(consumer::closeEx), ($, v) -> v).async());
	}

	public <T1, X1> StreamConsumerWithResult<T1, X1> transform(
			Function<StreamConsumer<T>, StreamConsumer<T1>> consumerTransformer,
			Function<Promise<X>, Promise<X1>> resultTransformer) {
		return new StreamConsumerWithResult<>(
				consumerTransformer.apply(consumer),
				resultTransformer.apply(result));
	}

	public <T1> StreamConsumerWithResult<T1, X> transformConsumer(Function<StreamConsumer<T>, StreamConsumer<T1>> consumerTransformer) {
		return transform(consumerTransformer, Function.identity());
	}

	public <X1> StreamConsumerWithResult<T, X1> transformResult(Function<Promise<X>, Promise<X1>> resultTransformer) {
		return transform(Function.identity(), resultTransformer);
	}

	public static <T, X> StreamConsumerWithResult<T, X> ofPromise(Promise<StreamConsumerWithResult<T, X>> promise) {
		return of(
				StreamConsumers.ofPromise(promise.map(StreamConsumerWithResult::getConsumer)),
				promise.then(StreamConsumerWithResult::getResult));
	}

	public StreamConsumer<T> getConsumer() {
		return consumer;
	}

	public Promise<X> getResult() {
		return result;
	}
}
