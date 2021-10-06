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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

public final class AsyncBiPredicates {

	@Contract(pure = true)
	public static <T, U> @NotNull AsyncBiPredicate<T, U> buffer(@NotNull AsyncBiPredicate<T, U> actual) {
		return buffer(1, Integer.MAX_VALUE, actual);
	}

	@Contract(pure = true)
	public static <T, U> @NotNull AsyncBiPredicate<T, U> buffer(int maxParallelCalls, int maxBufferedCalls, @NotNull AsyncBiPredicate<T, U> asyncPredicate) {
		return ofExecutor(AsyncExecutors.buffered(maxParallelCalls, maxBufferedCalls), asyncPredicate);
	}

	@Contract(pure = true)
	public static <T, U> @NotNull AsyncBiPredicate<T, U> ofExecutor(@NotNull AsyncExecutor asyncExecutor, @NotNull AsyncBiPredicate<T, U> predicate) {
		return (t, u) -> asyncExecutor.execute(() -> predicate.test(t, u));
	}

	/**
	 * Returns a composed {@link AsyncBiPredicate} that represents a logical AND of all predicates
	 * in a given collection
	 * <p>
	 * Unlike Java's {@link Predicate#and(Predicate)} this method does not provide short-circuit.
	 * If either predicate returns an exceptionally completed promise,
	 * the combined asynchronous predicate also returns an exceptionally completed promise
	 *
	 * @param predicates a collection of {@link AsyncBiPredicate}s that will be logically ANDed
	 * @return a composed {@link AsyncBiPredicate} that represents a logical AND of asynchronous predicates
	 * in a given collection
	 */
	public static <T, U> @NotNull AsyncBiPredicate<T, U> and(Collection<AsyncBiPredicate<? super T, ? super U>> predicates) {
		return (t, u) -> {
			AsyncAccumulator<RefBoolean> asyncAccumulator = AsyncAccumulator.create(new RefBoolean(true));
			for (AsyncBiPredicate<? super T, ? super U> predicate : predicates) {
				asyncAccumulator.addPromise(predicate.test(t, u), (ref, result) -> ref.set(ref.get() && result));
			}
			return asyncAccumulator.run().map(RefBoolean::get);
		};
	}

	/**
	 * Returns an {@link AsyncBiPredicate} that always returns promise of {@code true}
	 *
	 * @see #and(Collection)
	 */
	public static <T, U> @NotNull AsyncBiPredicate<T, U> and() {
		return AsyncBiPredicate.alwaysTrue();
	}

	/**
	 * Returns a given asynchronous predicate
	 * <p>
	 * A shortcut for {@link #and(AsyncBiPredicate[])}
	 *
	 * @see #and(Collection)
	 * @see #and(AsyncBiPredicate[])
	 */
	public static <T, U> @NotNull AsyncBiPredicate<T, U> and(AsyncBiPredicate<? super T, ? super U> predicate1) {
		//noinspection unchecked
		return (AsyncBiPredicate<T, U>) predicate1;
	}

	/**
	 * An optimization for {@link #and(AsyncBiPredicate[])}
	 *
	 * @see #and(Collection)
	 * @see #and(AsyncBiPredicate[])
	 */
	public static <T, U> @NotNull AsyncBiPredicate<T, U> and(AsyncBiPredicate<? super T, ? super U> predicate1, AsyncBiPredicate<? super T, ? super U> predicate2) {
		//noinspection unchecked
		return ((AsyncBiPredicate<T, U>) predicate1).and(predicate2);
	}

	/**
	 * Returns a composed {@link AsyncBiPredicate} that represents a logical AND of all predicates
	 * in a given array
	 *
	 * @see #and(Collection)
	 */
	@SafeVarargs
	public static <T, U> @NotNull AsyncBiPredicate<T, U> and(AsyncBiPredicate<? super T, ? super U>... predicates) {
		//noinspection RedundantCast
		return and(((List<AsyncBiPredicate<? super T, ? super U>>) Arrays.asList(predicates)));
	}

	/**
	 * Returns a composed {@link AsyncBiPredicate} that represents a logical OR of all predicates
	 * in a given collection
	 * <p>
	 * Unlike Java's {@link Predicate#or(Predicate)} this method does not provide short-circuit.
	 * If either predicate returns an exceptionally completed promise,
	 * the combined asynchronous predicate also returns an exceptionally completed promise
	 *
	 * @param predicates a collection of {@link AsyncBiPredicate}s that will be logically ORed
	 * @return a composed {@link AsyncBiPredicate} that represents a logical OR of asynchronous predicates
	 * in a given collection
	 */
	public static <T, U> @NotNull AsyncBiPredicate<T, U> or(Collection<AsyncBiPredicate<? super T, ? super U>> predicates) {
		return (t, u) -> {
			AsyncAccumulator<RefBoolean> asyncAccumulator = AsyncAccumulator.create(new RefBoolean(false));
			for (AsyncBiPredicate<? super T, ? super U> predicate : predicates) {
				asyncAccumulator.addPromise(predicate.test(t, u), (ref, result) -> ref.set(ref.get() || result));
			}
			return asyncAccumulator.run().map(RefBoolean::get);
		};
	}

	/**
	 * Returns an {@link AsyncBiPredicate} that always returns promise of {@code false}
	 *
	 * @see #or(Collection)
	 */
	public static <T, U> @NotNull AsyncBiPredicate<T, U> or() {
		return AsyncBiPredicate.alwaysFalse();
	}

	/**
	 * Returns a given asynchronous predicate
	 * <p>
	 * A shortcut for {@link #or(AsyncBiPredicate[])}
	 *
	 * @see #or(Collection)
	 * @see #or(AsyncBiPredicate[])
	 */
	public static <T, U> @NotNull AsyncBiPredicate<T, U> or(AsyncBiPredicate<? super T, ? super U> predicate1) {
		//noinspection unchecked
		return (AsyncBiPredicate<T, U>) predicate1;
	}

	/**
	 * An optimization for {@link #or(AsyncBiPredicate[])}
	 *
	 * @see #or(Collection)
	 * @see #or(AsyncBiPredicate[])
	 */
	public static <T, U> @NotNull AsyncBiPredicate<T, U> or(AsyncBiPredicate<? super T, ? super U> predicate1, AsyncBiPredicate<? super T, ? super U> predicate2) {
		//noinspection unchecked
		return ((AsyncBiPredicate<T, U>) predicate1).or(predicate2);
	}

	/**
	 * Returns a composed {@link AsyncBiPredicate} that represents a logical OR of all predicates
	 * in a given array
	 *
	 * @see #or(Collection)
	 */
	@SafeVarargs
	public static <T, U> @NotNull AsyncBiPredicate<T, U> or(AsyncBiPredicate<? super T, ? super U>... predicates) {
		//noinspection RedundantCast
		return or(((List<AsyncBiPredicate<? super T, ? super U>>) Arrays.asList(predicates)));
	}
}
