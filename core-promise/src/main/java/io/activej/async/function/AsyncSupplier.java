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

package io.activej.async.function;

import io.activej.async.process.AsyncExecutor;
import io.activej.common.collection.Try;
import io.activej.common.function.SupplierEx;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.activej.common.exception.FatalErrorHandlers.handleError;

/**
 * Represents asynchronous supplier that returns {@link Promise} of some data.
 */
@FunctionalInterface
public interface AsyncSupplier<T> {
	/**
	 * Gets {@link Promise} of data item asynchronously.
	 */
	Promise<T> get();

	/**
	 * Wraps a {@link SupplierEx} interface.
	 *
	 * @param supplier a {@link SupplierEx}
	 * @return {@link AsyncSupplier} that works on top of {@link SupplierEx} interface
	 */
	static <T> AsyncSupplier<T> of(@NotNull SupplierEx<Promise<T>> supplier) {
		return () -> {
			try {
				return supplier.get();
			} catch (Exception e) {
				handleError(e, supplier);
				return Promise.ofException(e);
			}
		};
	}

	static <T> AsyncSupplier<T> ofValue(@Nullable T value) {
		return () -> Promise.of(value);
	}

	static <T> AsyncSupplier<T> ofIterator(@NotNull Iterator<? extends T> iterator) {
		return () -> Promise.of(iterator.hasNext() ? iterator.next() : null);
	}

	static <T> AsyncSupplier<T> ofStream(@NotNull Stream<? extends T> stream) {
		return ofIterator(stream.iterator());
	}

	static <T> AsyncSupplier<T> ofIterable(@NotNull Iterable<? extends T> iterable) {
		return ofIterator(iterable.iterator());
	}

	static <T> AsyncSupplier<T> ofPromise(@NotNull Promise<T> promise) {
		return () -> promise;
	}

	static <T> AsyncSupplier<T> ofPromiseIterator(@NotNull Iterator<? extends Promise<T>> iterator) {
		return () -> iterator.hasNext() ? iterator.next() : Promise.of(null);
	}

	static <T> AsyncSupplier<T> ofPromiseIterable(@NotNull Iterable<? extends Promise<T>> iterable) {
		return ofPromiseIterator(iterable.iterator());
	}

	static <T> AsyncSupplier<T> ofPromiseStream(@NotNull Stream<? extends Promise<T>> stream) {
		return ofPromiseIterator(stream.iterator());
	}

	static <T> AsyncSupplier<T> ofAsyncSupplierIterator(@NotNull Iterator<? extends AsyncSupplier<T>> iterator) {
		return () -> iterator.hasNext() ? iterator.next().get() : Promise.of(null);
	}

	static <T> AsyncSupplier<T> ofAsyncSupplierIterable(@NotNull Iterable<? extends AsyncSupplier<T>> iterable) {
		return ofAsyncSupplierIterator(iterable.iterator());
	}

	static <T> AsyncSupplier<T> ofAsyncSupplierStream(@NotNull Stream<? extends AsyncSupplier<T>> stream) {
		return ofAsyncSupplierIterator(stream.iterator());
	}

	@Contract(pure = true)
	default @NotNull <R> R transformWith(@NotNull Function<AsyncSupplier<T>, R> fn) {
		return fn.apply(this);
	}

	/**
	 * Ensures that supplied {@code Promise} will complete asynchronously.
	 *
	 * @return {@link AsyncSupplier} of {@code Promise}s
	 * that will be completed asynchronously
	 * @see Promise#async()
	 */
	@Contract(pure = true)
	default @NotNull AsyncSupplier<T> async() {
		return () -> get().async();
	}

	@Contract(pure = true)
	default @NotNull AsyncSupplier<Try<T>> toTry() {
		return () -> get().toTry();
	}

	@Contract(pure = true)
	default @NotNull AsyncSupplier<T> withExecutor(@NotNull AsyncExecutor asyncExecutor) {
		return () -> asyncExecutor.execute(this);
	}
}
