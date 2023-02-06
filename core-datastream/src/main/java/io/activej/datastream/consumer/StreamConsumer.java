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

import io.activej.async.process.AsyncCloseable;
import io.activej.datastream.processor.transformer.StreamConsumerTransformer;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.util.function.UnaryOperator;

/**
 * This interface represents an object that can asynchronously receive streams of data.
 * <p>
 * Implementors of this interface might want to extend {@link AbstractStreamConsumer}
 * instead of this interface, since it makes the threading and state management easier.
 */
public interface StreamConsumer<T> extends AsyncCloseable {
	/**
	 * Begins streaming data from the given supplier into this consumer.
	 * This method may not be called directly, use {@link StreamSupplier#streamTo} instead.
	 * <p>
	 * This method must have no effect after {@link #getAcknowledgement() the acknowledgement} is set.
	 */
	void consume(StreamSupplier<T> streamSupplier);

	@Nullable StreamDataAcceptor<T> getDataAcceptor();

	/**
	 * A signal promise of the <i>acknowledgement</i> state of this consumer - its completion means that
	 * this consumer changed to that state and is now <b>closed</b>.
	 * <p>
	 * When the consumer is in this state nobody must send any more data to any of its related acceptors.
	 * <p>
	 * If promise completes with an error then this consumer closes with that error.
	 */
	Promise<Void> getAcknowledgement();

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
	 * Transforms this supplier with a given transformer.
	 */
	default <R> R transformWith(StreamConsumerTransformer<T, R> fn) {
		return fn.transform(this);
	}

	/**
	 * Creates a consumer from this one with its <i>acknowledge</i> signal modified by the given function.
	 */
	default StreamConsumer<T> withAcknowledgement(UnaryOperator<Promise<Void>> fn) {
		Promise<Void> acknowledgement = getAcknowledgement();
		Promise<Void> newAcknowledgement = fn.apply(acknowledgement);
		if (acknowledgement == newAcknowledgement) return this;
		return new ForwardingStreamConsumer<>(this) {
			@Override
			public Promise<Void> getAcknowledgement() {
				return newAcknowledgement;
			}
		};
	}
}
