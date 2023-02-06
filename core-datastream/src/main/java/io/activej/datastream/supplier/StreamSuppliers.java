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

import io.activej.common.annotation.StaticFactories;
import io.activej.common.function.SupplierEx;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.processor.transformer.StreamTransformer;
import io.activej.datastream.processor.transformer.StreamTransformers;
import io.activej.datastream.supplier.impl.*;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;

import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Arrays.asList;

@StaticFactories(StreamSupplier.class)
public final class StreamSuppliers {
	/**
	 * Creates a supplier which supplies items that were sent into the consumer received through the callback.
	 */
	public static <T> StreamSupplier<T> ofConsumer(Consumer<StreamConsumer<T>> consumer) {
		StreamTransformer<T, T> forwarder = StreamTransformers.identity();
		consumer.accept(forwarder.getInput());
		return forwarder.getOutput();
	}

	/**
	 * Creates a supplier which does not send any data and never moves to the closed state.
	 */
	public static <T> StreamSupplier<T> idle() {
		return new Idle<>();
	}

	/**
	 * Creates a supplier that is in the closed state with given error set.
	 */
	public static <T> StreamSupplier<T> closingWithError(Exception e) {
		return new ClosingWithError<>(e);
	}

	/**
	 * Creates an empty supplier that is in the closed state immediately.
	 */
	public static <T> StreamSupplier<T> empty() {
		return new Empty<>();
	}

	/**
	 * Creates a supplier that supplies a single value.
	 */
	public static <T> StreamSupplier<T> ofValue(T value) {
		return new OfValue<>(value);
	}

	/**
	 * @see #empty()
	 */
	public static <T> StreamSupplier<T> ofValues() {
		return empty();
	}

	/**
	 * @see #empty()
	 */
	public static <T> StreamSupplier<T> ofValues(T value) {
		return ofValue(value);
	}

	/**
	 * Creates a supplier which supplies given items and then closes.
	 */
	@SafeVarargs
	public static <T> StreamSupplier<T> ofValues(T... items) {
		return new OfIterator<>(asList(items).iterator());
	}

	/**
	 * Creates a supplier which supplies items from the given iterator and then closes.
	 */
	public static <T> StreamSupplier<T> ofIterator(Iterator<T> iterator) {
		return new OfIterator<>(iterator);
	}

	/**
	 * Creates a supplier which supplies items from the given iterable and then closes.
	 */
	public static <T> StreamSupplier<T> ofIterable(Iterable<T> iterable) {
		return new OfIterator<>(iterable.iterator());
	}

	/**
	 * Creates a supplier which supplies items from the given stream and then closes.
	 */
	public static <T> StreamSupplier<T> ofStream(Stream<T> stream) {
		return new OfIterator<>(stream.iterator());
	}

	/**
	 * Creates a supplier which supplies items by calling a given lambda.
	 * It closes itself (and changes to closed state) when lambda returns <code>null</code>.
	 */
	public static <T> StreamSupplier<T> ofSupplier(SupplierEx<T> supplier) {
		return new OfSupplier<>(supplier);
	}

	/**
	 * Creates a supplier which supplies items from the given channel supplier and then closes.
	 */
	public static <T> StreamSupplier<T> ofChannelSupplier(ChannelSupplier<T> supplier) {
		return new OfChannelSupplier<>(supplier);
	}

	/**
	 * Creates a supplier that waits until the promise completes
	 * and then supplies items from the resulting supplier.
	 */
	public static <T> StreamSupplier<T> ofPromise(Promise<? extends StreamSupplier<T>> promise) {
		if (promise.isResult()) return promise.getResult();
		return new OfPromise<>(promise);
	}

	public static <T> StreamSupplier<T> ofAnotherReactor(Reactor anotherReactor, StreamSupplier<T> anotherReactorSupplier) {
		if (Reactor.getCurrentReactor() == anotherReactor) {
			return anotherReactorSupplier;
		}
		return new OfAnotherReactor<>(anotherReactor, anotherReactorSupplier);
	}

	/**
	 * Creates a supplier that supplies items from given suppliers consecutively and only then closes.
	 */
	public static <T> StreamSupplier<T> concat(Iterator<StreamSupplier<T>> iterator) {
		return new Concat<>(ChannelSuppliers.ofIterator(iterator));
	}

	/**
	 * Creates a supplier that supplies items from given suppliers consecutively and only then closes.
	 */
	public static <T> StreamSupplier<T> concat(ChannelSupplier<StreamSupplier<T>> supplier) {
		return new Concat<>(supplier);
	}

	/**
	 * A shortcut for {@link #concat(Iterator)} that uses a list of suppliers
	 */
	public static <T> StreamSupplier<T> concat(List<StreamSupplier<T>> suppliers) {
		return new Concat<>(ChannelSuppliers.ofList(suppliers));
	}

	/**
	 * A shortcut for {@link #concat(Iterator)} that uses given suppliers
	 */
	@SafeVarargs
	public static <T> StreamSupplier<T> concat(StreamSupplier<T>... suppliers) {
		return concat(List.of(suppliers));
	}
}
