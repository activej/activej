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

package io.activej.async.process;

import io.activej.common.exception.UncheckedException;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.activej.common.Preconditions.checkState;

@SuppressWarnings("UnusedReturnValue")
public final class AsyncCollector<R> implements AsyncCloseable {
	@FunctionalInterface
	public interface Accumulator<R, T> {
		void accumulate(R result, T value) throws UncheckedException;
	}

	private final SettablePromise<R> resultPromise = new SettablePromise<>();
	private boolean started;

	@Nullable
	private R result;

	private int activePromises;

	public AsyncCollector(@Nullable R initialResult) {
		this.result = initialResult;
	}

	public static <R> AsyncCollector<R> create(@Nullable R initialResult) {
		return new AsyncCollector<>(initialResult);
	}

	public <T> AsyncCollector<R> withPromise(@NotNull Promise<T> promise, @NotNull Accumulator<R, T> accumulator) {
		addPromise(promise, accumulator);
		return this;
	}

	public AsyncCollector<R> run() {
		checkState(!started);
		this.started = true;
		if (activePromises == 0 && !resultPromise.isComplete()) {
			resultPromise.set(result);
			result = null;
		}
		return this;
	}

	public AsyncCollector<R> run(@NotNull Promise<Void> runtimePromise) {
		withPromise(runtimePromise, (result, v) -> {});
		return run();
	}

	@SuppressWarnings("unchecked")
	public <T> Promise<T> addPromise(@NotNull Promise<T> promise, @NotNull Accumulator<R, T> accumulator) {
		if (resultPromise.isException()) return (Promise<T>) resultPromise;
		checkState(!resultPromise.isComplete());
		activePromises++;
		return promise.whenComplete((v, e) -> {
			activePromises--;
			if (resultPromise.isComplete()) return;
			if (e == null) {
				try {
					accumulator.accumulate(result, v);
				} catch (UncheckedException u) {
					resultPromise.setException(u.getCause());
					result = null;
					return;
				}
				if (activePromises == 0 && started) {
					resultPromise.set(result);
				}
			} else {
				resultPromise.setException(e);
				result = null;
			}
		});
	}

	public <V> SettablePromise<V> newPromise(@NotNull Accumulator<R, V> accumulator) {
		SettablePromise<V> resultPromise = new SettablePromise<>();
		addPromise(resultPromise, accumulator);
		return resultPromise;
	}

	@NotNull
	public Promise<R> get() {
		return resultPromise;
	}

	public int getActivePromises() {
		return activePromises;
	}

	@Override
	public void closeEx(@NotNull Throwable e) {
		if (resultPromise.trySetException(e)) {
			result = null;
		}
	}
}
