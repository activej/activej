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

import io.activej.async.AsyncAccumulator;
import io.activej.async.process.AsyncExecutor;
import io.activej.async.process.AsyncExecutors;
import io.activej.common.ref.RefBoolean;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

public final class AsyncPredicates {

	@Contract(pure = true)
	public static <T> @NotNull AsyncPredicate<T> buffer(@NotNull AsyncPredicate<T> actual) {
		return buffer(1, Integer.MAX_VALUE, actual);
	}

	@Contract(pure = true)
	public static <T> @NotNull AsyncPredicate<T> buffer(int maxParallelCalls, int maxBufferedCalls, @NotNull AsyncPredicate<T> asyncPredicate) {
		return ofExecutor(AsyncExecutors.buffered(maxParallelCalls, maxBufferedCalls), asyncPredicate);
	}

	@Contract(pure = true)
	public static <T> @NotNull AsyncPredicate<T> ofExecutor(@NotNull AsyncExecutor asyncExecutor, @NotNull AsyncPredicate<T> predicate) {
		return t -> asyncExecutor.execute(() -> predicate.test(t));
	}

	/**
	 * Returns a composed {@link AsyncPredicate} that represents a logical AND of all predicates
	 * in a given collection
	 * <p>
	 * Unlike Java's {@link Predicate#and(Predicate)} this method does not provide short-circuit.
	 * If either predicate returns an exceptionally completed promise,
	 * the combined asynchronous predicate also returns an exceptionally completed promise
	 *
	 * @param predicates a collection of {@link AsyncPredicate}s that will be logically ANDed
	 * @return a composed {@link AsyncPredicate} that represents a logical AND of asynchronous predicates
	 * in a given collection
	 */
	public static <T> @NotNull AsyncPredicate<T> and(Collection<AsyncPredicate<? super T>> predicates) {
		return t -> {
			AsyncAccumulator<RefBoolean> asyncAccumulator = AsyncAccumulator.create(new RefBoolean(true));
			for (AsyncPredicate<? super T> predicate : predicates) {
				asyncAccumulator.addPromise(predicate.test(t), (ref, result) -> ref.set(ref.get() && result));
			}
			return asyncAccumulator.run().map(RefBoolean::get);
		};
	}

	/**
	 * Returns an {@link AsyncPredicate} that always returns promise of {@code true}
	 *
	 * @see #and(Collection)
	 */
	public static <T> @NotNull AsyncPredicate<T> and() {
		return AsyncPredicate.alwaysTrue();
	}

	/**
	 * Returns a given asynchronous predicate
	 * <p>
	 * A shortcut for {@link #and(AsyncPredicate[])}
	 *
	 * @see #and(Collection)
	 * @see #and(AsyncPredicate[])
	 */
	public static <T> @NotNull AsyncPredicate<T> and(AsyncPredicate<? super T> predicate1) {
		//noinspection unchecked
		return (AsyncPredicate<T>) predicate1;
	}

	/**
	 * An optimization for {@link #and(AsyncPredicate[])}
	 *
	 * @see #and(Collection)
	 * @see #and(AsyncPredicate[])
	 */
	public static <T> @NotNull AsyncPredicate<T> and(AsyncPredicate<? super T> predicate1, AsyncPredicate<? super T> predicate2) {
		//noinspection unchecked
		return ((AsyncPredicate<T>) predicate1).and(predicate2);
	}

	/**
	 * Returns a composed {@link AsyncPredicate} that represents a logical AND of all predicates
	 * in a given array
	 *
	 * @see #and(Collection)
	 */
	@SafeVarargs
	public static <T> @NotNull AsyncPredicate<T> and(AsyncPredicate<? super T>... predicates) {
		//noinspection RedundantCast
		return and(((List<AsyncPredicate<? super T>>) List.of(predicates)));
	}

	/**
	 * Returns a composed {@link AsyncPredicate} that represents a logical OR of all predicates
	 * in a given collection
	 * <p>
	 * Unlike Java's {@link Predicate#or(Predicate)} this method does not provide short-circuit.
	 * If either predicate returns an exceptionally completed promise,
	 * the combined asynchronous predicate also returns an exceptionally completed promise
	 *
	 * @param predicates a collection of {@link AsyncPredicate}s that will be logically ORed
	 * @return a composed {@link AsyncPredicate} that represents a logical OR of asynchronous predicates
	 * in a given collection
	 */
	public static <T> @NotNull AsyncPredicate<T> or(Collection<AsyncPredicate<? super T>> predicates) {
		return t -> {
			AsyncAccumulator<RefBoolean> asyncAccumulator = AsyncAccumulator.create(new RefBoolean(false));
			for (AsyncPredicate<? super T> predicate : predicates) {
				asyncAccumulator.addPromise(predicate.test(t), (ref, result) -> ref.set(ref.get() || result));
			}
			return asyncAccumulator.run().map(RefBoolean::get);
		};
	}

	/**
	 * @return an {@link AsyncPredicate} that always returns promise of {@code false}
	 * @see #or(Collection)
	 */
	public static <T> @NotNull AsyncPredicate<T> or() {
		return AsyncPredicate.alwaysFalse();
	}

	/**
	 * Returns a given asynchronous predicate
	 * <p>
	 * A shortcut for {@link #or(AsyncPredicate[])}
	 *
	 * @see #or(Collection)
	 * @see #or(AsyncPredicate[])
	 */
	public static <T> @NotNull AsyncPredicate<T> or(AsyncPredicate<? super T> predicate1) {
		//noinspection unchecked
		return (AsyncPredicate<T>) predicate1;
	}

	/**
	 * An optimization for {@link #or(AsyncPredicate[])}
	 *
	 * @see #or(Collection)
	 * @see #or(AsyncPredicate[])
	 */
	public static <T> @NotNull AsyncPredicate<T> or(AsyncPredicate<? super T> predicate1, AsyncPredicate<? super T> predicate2) {
		//noinspection unchecked
		return ((AsyncPredicate<T>) predicate1).or(predicate2);
	}

	/**
	 * Returns a composed {@link AsyncPredicate} that represents a logical OR of all predicates
	 * in a given array
	 *
	 * @see #or(Collection)
	 */
	@SafeVarargs
	public static <T> @NotNull AsyncPredicate<T> or(AsyncPredicate<? super T>... predicates) {
		//noinspection RedundantCast
		return or(((List<AsyncPredicate<? super T>>) List.of(predicates)));
	}
}
