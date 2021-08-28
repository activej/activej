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

package io.activej.async;

import io.activej.async.process.AsyncCloseable;
import io.activej.common.exception.UncheckedException;
import io.activej.common.function.ThrowingBiConsumer;
import io.activej.common.recycle.Recyclers;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiConsumer;

import static io.activej.common.Checks.checkState;

@SuppressWarnings("UnusedReturnValue")
public final class AsyncAccumulator<A> implements AsyncCloseable {
	private final SettablePromise<A> resultPromise = new SettablePromise<>();
	private boolean started;

	private final A accumulator;

	private int activePromises;

	private AsyncAccumulator(@Nullable A accumulator) {
		this.accumulator = accumulator;
	}

	public static <A> AsyncAccumulator<A> create(@Nullable A accumulator) {
		return new AsyncAccumulator<>(accumulator);
	}

	public <T> AsyncAccumulator<A> withPromise(@NotNull Promise<T> promise, @NotNull ThrowingBiConsumer<A, T> accumulator) {
		addPromise(promise, accumulator);
		return this;
	}

	public Promise<A> run() {
		checkState(!started);
		this.started = true;
		if (resultPromise.isComplete()) return resultPromise;
		if (activePromises == 0) {
			resultPromise.set(accumulator);
		}
		return resultPromise;
	}

	public Promise<A> run(@NotNull Promise<Void> runtimePromise) {
		addPromise(runtimePromise, (result, v) -> {});
		return run();
	}

	public <T> void addPromise(@NotNull Promise<T> promise, @NotNull ThrowingBiConsumer<A, T> consumer) {
		if (resultPromise.isComplete()) {
			promise.whenResult(Recyclers::recycle);
			return;
		}
		activePromises++;
		promise.whenComplete((v, e) -> {
			activePromises--;
			if (resultPromise.isComplete()) {
				Recyclers.recycle(v);
				return;
			}
			if (e == null) {
				try {
					consumer.accept(accumulator, v);
				} catch (Exception e0) {
					resultPromise.setException(e0);
					Recyclers.recycle(accumulator);
					return;
				}
				if (activePromises == 0 && started) {
					resultPromise.set(accumulator);
				}
			} else {
				resultPromise.setException(e);
				Recyclers.recycle(accumulator);
			}
		});
	}

	public <V> SettablePromise<V> newPromise(@NotNull ThrowingBiConsumer<A, V> consumer) {
		SettablePromise<V> resultPromise = new SettablePromise<>();
		addPromise(resultPromise, consumer);
		return resultPromise;
	}

	@NotNull
	public Promise<A> get() {
		return resultPromise;
	}

	public A getAccumulator() {
		return accumulator;
	}

	public int getActivePromises() {
		return activePromises;
	}

	public void complete() {
		resultPromise.trySet(accumulator);
	}

	public void complete(A result) {
		if (resultPromise.trySet(result)) {
			if (result != accumulator) {
				Recyclers.recycle(accumulator);
			}
		}
	}

	@Override
	public void closeEx(@NotNull Throwable e) {
		if (resultPromise.trySetException(e)) {
			Recyclers.recycle(accumulator);
		}
	}
}
