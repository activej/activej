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
import io.activej.common.Checks;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.function.BiConsumerEx;
import io.activej.common.recycle.Recyclers;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.ImplicitlyReactive;
import org.jetbrains.annotations.Nullable;

import static io.activej.common.Checks.checkState;
import static io.activej.common.exception.FatalErrorHandler.handleError;
import static io.activej.reactor.Reactive.checkInReactorThread;

@SuppressWarnings("UnusedReturnValue")
public final class AsyncAccumulator<A> extends ImplicitlyReactive implements AsyncCloseable {
	private static final boolean CHECKS = Checks.isEnabled(AsyncAccumulator.class);

	private final SettablePromise<A> resultPromise = new SettablePromise<>();
	private boolean started;

	private final A accumulator;

	private int activePromises;

	private AsyncAccumulator(@Nullable A accumulator) {
		this.accumulator = accumulator;
	}

	public static <A> AsyncAccumulator<A> create(@Nullable A accumulator) {
		return builder(accumulator).build();
	}

	public static <A> AsyncAccumulator<A>.Builder builder(@Nullable A accumulator) {
		return new AsyncAccumulator<>(accumulator).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, AsyncAccumulator<A>> {
		private Builder() {}

		public <T> Builder withPromise(Promise<T> promise, BiConsumerEx<A, T> accumulator) {
			checkNotBuilt(this);
			addPromise(promise, accumulator);
			return this;
		}

		@Override
		protected AsyncAccumulator<A> doBuild() {
			return AsyncAccumulator.this;
		}
	}

	public Promise<A> run() {
		checkInReactorThread(this);
		checkState(!started);
		this.started = true;
		if (resultPromise.isComplete()) return resultPromise;
		if (activePromises == 0) {
			resultPromise.set(accumulator);
		}
		return resultPromise;
	}

	public Promise<A> run(Promise<Void> runtimePromise) {
		checkInReactorThread(this);
		addPromise(runtimePromise, (result, v) -> {});
		return run();
	}

	public <T> void addPromise(Promise<T> promise, BiConsumerEx<A, T> consumer) {
		if (CHECKS) checkInReactorThread(this);
		if (resultPromise.isComplete()) {
			promise.whenResult(Recyclers::recycle);
			return;
		}
		activePromises++;
		promise.subscribe((v, e) -> {
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
		if (CHECKS) checkInReactorThread(this);
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
		checkInReactorThread(this);
		resultPromise.trySet(accumulator);
	}

	public void complete(A result) {
		checkInReactorThread(this);
		if (resultPromise.trySet(result) && result != accumulator) {
			Recyclers.recycle(accumulator);
		}
	}

	@Override
	public void closeEx(Exception e) {
		checkInReactorThread(this);
		if (resultPromise.trySetException(e)) {
			Recyclers.recycle(accumulator);
		}
	}
}
