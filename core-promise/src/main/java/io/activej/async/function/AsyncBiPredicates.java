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
import io.activej.async.process.AsyncExecutors;
import io.activej.common.ref.RefBoolean;
import io.activej.promise.Promises;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collection;

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

	public static <T, U> @NotNull AsyncBiPredicate<T, U> and(Collection<AsyncBiPredicate<? super T, ? super U>> predicates) {
		return (t, u) -> Promises.reduce(new RefBoolean(true),
				(ref, result) -> ref.set(ref.get() && result),
				RefBoolean::get,
				predicates.stream().map(predicate -> predicate.test(t, u)));
	}

	public static <T, U> @NotNull AsyncBiPredicate<T, U> and() {
		return AsyncBiPredicate.alwaysTrue();
	}

	public static <T, U> @NotNull AsyncBiPredicate<T, U> and(AsyncBiPredicate<? super T, ? super U> predicate1) {
		//noinspection unchecked
		return (AsyncBiPredicate<T, U>) predicate1;
	}

	public static <T, U> @NotNull AsyncBiPredicate<T, U> and(AsyncBiPredicate<? super T, ? super U> predicate1, AsyncBiPredicate<? super T, ? super U> predicate2) {
		//noinspection unchecked
		return ((AsyncBiPredicate<T, U>) predicate1).and(predicate2);
	}

	@SafeVarargs
	public static <T, U> @NotNull AsyncBiPredicate<T, U> and(AsyncBiPredicate<? super T, ? super U>... predicates) {
		return and(Arrays.asList(predicates));
	}

	public static <T, U> @NotNull AsyncBiPredicate<T, U> or(Collection<AsyncBiPredicate<? super T, ? super U>> predicates) {
		return (t, u) -> Promises.reduce(new RefBoolean(false),
				(ref, result) -> ref.set(ref.get() || result),
				RefBoolean::get,
				predicates.stream().map(predicate -> predicate.test(t, u)));
	}

	public static <T, U> @NotNull AsyncBiPredicate<T, U> or() {
		return AsyncBiPredicate.alwaysFalse();
	}

	public static <T, U> @NotNull AsyncBiPredicate<T, U> or(AsyncBiPredicate<? super T, ? super U> predicate1) {
		//noinspection unchecked
		return (AsyncBiPredicate<T, U>) predicate1;
	}

	public static <T, U> @NotNull AsyncBiPredicate<T, U> or(AsyncBiPredicate<? super T, ? super U> predicate1, AsyncBiPredicate<? super T, ? super U> predicate2) {
		//noinspection unchecked
		return ((AsyncBiPredicate<T, U>) predicate1).or(predicate2);
	}

	@SafeVarargs
	public static <T, U> @NotNull AsyncBiPredicate<T, U> or(AsyncBiPredicate<? super T, ? super U>... predicates) {
		return or(Arrays.asList(predicates));
	}
}
