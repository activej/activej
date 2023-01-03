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
import io.activej.common.function.BiConsumerEx;
import io.activej.common.initializer.WithInitializer;
import io.activej.common.recycle.Recyclers;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.ImplicitlyReactive;
import org.jetbrains.annotations.Nullable;

import static io.activej.common.Checks.checkState;
import static io.activej.common.exception.FatalErrorHandlers.handleError;

@SuppressWarnings("UnusedReturnValue")
public final class AsyncAccumulator<A> extends ImplicitlyReactive implements AsyncCloseable, WithInitializer<AsyncAccumulator<A>> {
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

	public <T> AsyncAccumulator<A> withPromise(Promise<T> promise, BiConsumerEx<A, T> accumulator) {
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

	public Promise<A> run(Promise<Void> runtimePromise) {
		addPromise(runtimePromise, (result, v) -> {});
		return run();
	}

	public <T> void addPromise(Promise<T> promise, BiConsumerEx<A, T> consumer) {
		if (resultPromise.isComplete()) {
			promise.whenResult(Recyclers::recycle);
			return;
		}
		activePromises++;
		promise.run((v, e) -> {
			activePromises--;
			if (resultPromise.isComplete()) {
				Recyclers.recycle(v);
				return;
			}
			if (e == null) {
				try {
					consumer.accept(accumulator, v);
				} catch (Exception ex) {
					handleError(ex, this);
					resultPromise.setException(ex);
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

	public <V> SettablePromise<V> newPromise(BiConsumerEx<A, V> consumer) {
		SettablePromise<V> resultPromise = new SettablePromise<>();
		addPromise(resultPromise, consumer);
		return resultPromise;
	}

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
		if (resultPromise.trySet(result) && result != accumulator) {
			Recyclers.recycle(accumulator);
		}
	}

	@Override
	public void closeEx(Exception e) {
		if (resultPromise.trySetException(e)) {
			Recyclers.recycle(accumulator);
		}
	}
}
