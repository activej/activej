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
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

public final class AsyncSuppliers {

	@Contract(pure = true)
	public static <T> @NotNull AsyncSupplier<T> reuse(@NotNull AsyncSupplier<? extends T> actual) {
		return new AsyncSupplier<T>() {
			@Nullable Promise<T> runningPromise;

			@SuppressWarnings("unchecked")
			@Override
			public @NotNull Promise<T> get() {
				if (runningPromise != null) return runningPromise;
				runningPromise = (Promise<T>) actual.get();
				Promise<T> runningPromise = this.runningPromise;
				runningPromise.whenComplete(() -> this.runningPromise = null);
				return runningPromise;
			}
		};
	}

	@Contract(pure = true)
	public static <T> @NotNull AsyncSupplier<T> coalesce(@NotNull AsyncSupplier<T> actual) {
		AsyncFunction<Void, T> fn = Promises.coalesce(() -> null, (a, v) -> {}, a -> actual.get());
		return () -> fn.apply(null);
	}

	@Contract(pure = true)
	public static <T> @NotNull AsyncSupplier<T> buffer(@NotNull AsyncSupplier<T> actual) {
		return buffer(1, Integer.MAX_VALUE, actual);
	}

	@Contract(pure = true)
	public static <T> @NotNull AsyncSupplier<T> buffer(int maxParallelCalls, int maxBufferedCalls, @NotNull AsyncSupplier<T> actualSupplier) {
		return ofExecutor(AsyncExecutors.buffered(maxParallelCalls, maxBufferedCalls), actualSupplier);
	}

	@Contract(pure = true)
	public static <T> @NotNull AsyncSupplier<T> ofExecutor(@NotNull AsyncExecutor asyncExecutor, @NotNull AsyncSupplier<T> supplier) {
		return () -> asyncExecutor.execute(supplier);
	}

	@Contract(pure = true)
	public static <T> AsyncSupplier<T> prefetch(int count, @NotNull AsyncSupplier<? extends T> asyncSupplier) {
		return prefetch(count, asyncSupplier, asyncSupplier);
	}

	@Contract(pure = true)
	@SuppressWarnings("unchecked")
	public static <T> AsyncSupplier<T> prefetch(int count,
			@NotNull AsyncSupplier<? extends T> actualSupplier,
			@NotNull AsyncSupplier<? extends T> prefetchSupplier) {
		if (count == 0) return (AsyncSupplier<T>) actualSupplier;
		return new AsyncSupplier<T>() {
			final ArrayDeque<T> prefetched = new ArrayDeque<>();
			int prefetchCalls;

			@SuppressWarnings("unchecked")
			@Override
			public @NotNull Promise<T> get() {
				Promise<? extends T> result = prefetched.isEmpty() ? actualSupplier.get() : Promise.of(prefetched.pollFirst());
				prefetch();
				return (Promise<T>) result;
			}

			void prefetch() {
				for (int i = 0; i < count - (prefetched.size() + prefetchCalls); i++) {
					prefetchCalls++;
					prefetchSupplier.get()
							.async()
							.run((value, e) -> {
								prefetchCalls--;
								if (e == null) {
									prefetched.addLast(value);
								}
							});
				}
			}
		};
	}

}
