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

import io.activej.async.process.AsyncCloseable;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.StreamConsumerWithResult;
import io.activej.datastream.consumer.StreamConsumers;
import io.activej.datastream.consumer.ToCollectorStreamConsumer;
import io.activej.datastream.processor.transformer.StreamSupplierTransformer;
import io.activej.promise.Promise;

import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * This interface represents an object that can asynchronously send streams of data.
 * <p>
 * Implementors of this interface might want to extend {@link AbstractStreamSupplier}
 * instead of this interface, since it makes the threading and state management easier.
 */
public interface StreamSupplier<T> extends AsyncCloseable {
	/**
	 * Bind this supplier to given {@link StreamConsumer} and start streaming
	 * data through them following all the contracts.
	 */
	Promise<Void> streamTo(StreamConsumer<T> consumer);

	Promise<Void> getAcknowledgement();

	/**
	 * A shortcut for {@link #streamTo(StreamConsumer)} that uses a promise of a stream.
	 */
	default Promise<Void> streamTo(Promise<StreamConsumer<T>> consumerPromise) {
		return streamTo(StreamConsumers.ofPromise(consumerPromise));
	}

	void updateDataAcceptor();

	/**
	 * A signal promise of the <i>end of stream</i> state of this supplier - its completion means that
	 * this supplier changed to that state and is now <b>closed</b>.
	 * <p>
	 * In this state supplier <b>must not</b> supply anything to any acceptors (just like when suspended).
	 * <p>
	 * If promise completes with an error then this supplier closes with that error.
	 */
	Promise<Void> getEndOfStream();

	default boolean isComplete() {
		return getAcknowledgement().isComplete();
	}

	default boolean isResult() {
		return getAcknowledgement().isResult();
	}

	default boolean isException() {
		return getAcknowledgement().isException();
	}

	/**
	 * A shortcut for {@link #streamTo(StreamConsumer)} for {@link StreamConsumerWithResult}.
	 */
	default <X> Promise<X> streamTo(StreamConsumerWithResult<T, X> consumerWithResult) {
		return streamTo(consumerWithResult.getConsumer())
				.then(consumerWithResult::getResult);
	}

	/**
	 * Transforms this supplier with a given transformer.
	 */
	default <R> R transformWith(StreamSupplierTransformer<T, R> fn) {
		return fn.transform(this);
	}

	/**
	 * Accumulates items from this supplier until it closes and
	 * then completes the returned promise with the accumulator.
	 */
	default <A, R> Promise<R> toCollector(Collector<T, A, R> collector) {
		ToCollectorStreamConsumer<T, A, R> consumerToCollector = ToCollectorStreamConsumer.create(collector);
		this.streamTo(consumerToCollector);
		return consumerToCollector.getResultPromise();
	}

	/**
	 * A shortcut for {@link #toCollector} that accumulates to a {@link List}.
	 */
	default Promise<List<T>> toList() {
		return toCollector(Collectors.toList());
	}

	/**
	 * Creates a supplier from this one with its <i>end of stream</i> signal modified by the given function.
	 */
	default StreamSupplier<T> withEndOfStream(UnaryOperator<Promise<Void>> fn) {
		Promise<Void> endOfStream = getEndOfStream();
		Promise<Void> suppliedEndOfStream = fn.apply(endOfStream);
		if (endOfStream == suppliedEndOfStream) {
			return this;
		}
		return new ForwardingStreamSupplier<>(this) {
			@Override
			public Promise<Void> getEndOfStream() {
				return suppliedEndOfStream;
			}
		};
	}

}
