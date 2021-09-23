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

	public static <T> @NotNull AsyncPredicate<T> and(Collection<AsyncPredicate<? super T>> predicates) {
		return t -> {
			AsyncAccumulator<RefBoolean> asyncAccumulator = AsyncAccumulator.create(new RefBoolean(true));
			for (AsyncPredicate<? super T> predicate : predicates) {
				asyncAccumulator.addPromise(predicate.test(t), (ref, result) -> ref.set(ref.get() && result));
			}
			return asyncAccumulator.run().map(RefBoolean::get);
		};
	}

	public static <T> @NotNull AsyncPredicate<T> and() {
		return AsyncPredicate.alwaysTrue();
	}

	public static <T> @NotNull AsyncPredicate<T> and(AsyncPredicate<? super T> predicate1) {
		//noinspection unchecked
		return (AsyncPredicate<T>) predicate1;
	}

	public static <T> @NotNull AsyncPredicate<T> and(AsyncPredicate<? super T> predicate1, AsyncPredicate<? super T> predicate2) {
		//noinspection unchecked
		return ((AsyncPredicate<T>) predicate1).and(predicate2);
	}

	@SafeVarargs
	public static <T> @NotNull AsyncPredicate<T> and(AsyncPredicate<? super T>... predicates) {
		//noinspection RedundantCast
		return and(((List<AsyncPredicate<? super T>>) Arrays.asList(predicates)));
	}

	public static <T> @NotNull AsyncPredicate<T> or(Collection<AsyncPredicate<? super T>> predicates) {
		return t -> {
			AsyncAccumulator<RefBoolean> asyncAccumulator = AsyncAccumulator.create(new RefBoolean(false));
			for (AsyncPredicate<? super T> predicate : predicates) {
				asyncAccumulator.addPromise(predicate.test(t), (ref, result) -> ref.set(ref.get() || result));
			}
			return asyncAccumulator.run().map(RefBoolean::get);
		};
	}

	public static <T> @NotNull AsyncPredicate<T> or() {
		return AsyncPredicate.alwaysFalse();
	}

	public static <T> @NotNull AsyncPredicate<T> or(AsyncPredicate<? super T> predicate1) {
		//noinspection unchecked
		return (AsyncPredicate<T>) predicate1;
	}

	public static <T> @NotNull AsyncPredicate<T> or(AsyncPredicate<? super T> predicate1, AsyncPredicate<? super T> predicate2) {
		//noinspection unchecked
		return ((AsyncPredicate<T>) predicate1).or(predicate2);
	}

	@SafeVarargs
	public static <T> @NotNull AsyncPredicate<T> or(AsyncPredicate<? super T>... predicates) {
		//noinspection RedundantCast
		return or(((List<AsyncPredicate<? super T>>) Arrays.asList(predicates)));
	}
}
