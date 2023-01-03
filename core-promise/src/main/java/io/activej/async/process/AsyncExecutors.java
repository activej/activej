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

import io.activej.async.function.AsyncSupplier;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.RetryPolicy;
import io.activej.promise.SettablePromise;
import io.activej.reactor.Reactor;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import static io.activej.common.Checks.checkArgument;
import static io.activej.reactor.util.RunnableWithContext.wrapContext;

public class AsyncExecutors {

	public static AsyncExecutor direct() {
		return new AsyncExecutor() {
			@Override
			public <T> Promise<T> execute(AsyncSupplier<T> supplier) {
				return supplier.get();
			}
		};
	}

	public static AsyncExecutor ofReactor(Reactor reactor) {
		return new AsyncExecutor() {
			@Override
			public <T> Promise<T> execute(AsyncSupplier<T> supplier) {
				Reactor currentReactor = Reactor.getCurrentReactor();
				if (reactor == currentReactor) {
					return supplier.get();
				}
				return Promise.ofCallback(cb -> {
					currentReactor.startExternalTask();
					reactor.execute(wrapContext(cb, () -> supplier.get()
							.run((result, e) -> {
								currentReactor.execute(wrapContext(cb, () -> cb.accept(result, e)));
								currentReactor.completeExternalTask();
							})));
				});
			}
		};
	}

	public static AsyncExecutor roundRobin(List<AsyncExecutor> executors) {
		return new AsyncExecutor() {
			int index;

			@Override
			public <T> Promise<T> execute(AsyncSupplier<T> supplier) {
				AsyncExecutor executor = executors.get(index);
				index = (index + 1) % executors.size();
				return executor.execute(supplier);
			}
		};
	}

	public static AsyncExecutor sequential() {
		return buffered(1, Integer.MAX_VALUE);
	}

	public static AsyncExecutor buffered(int maxParallelCalls) {
		return buffered(maxParallelCalls, Integer.MAX_VALUE);
	}

	public static AsyncExecutor buffered(int maxParallelCalls, int maxBufferedCalls) {
		return new AsyncExecutor() {
			private int pendingCalls;
			private final ArrayDeque<Object> deque = new ArrayDeque<>();

			@SuppressWarnings({"unchecked", "ConstantConditions"})
			private void processBuffer() {
				while (pendingCalls < maxParallelCalls && !deque.isEmpty()) {
					AsyncSupplier<Object> supplier = (AsyncSupplier<Object>) deque.pollFirst();
					SettablePromise<Object> cb = (SettablePromise<Object>) deque.pollFirst();
					pendingCalls++;
					Promise<Object> promise = supplier.get();
					if (promise.isComplete()){
						pendingCalls--;
						cb.accept(promise.getResult(), promise.getException());
						continue;
					}
					promise
							.run((result, e) -> {
								pendingCalls--;
								processBuffer();
								cb.accept(result, e);
							});
				}
			}

			@Override
			public <T> Promise<T> execute(AsyncSupplier<T> supplier) throws RejectedExecutionException {
				if (pendingCalls < maxParallelCalls) {
					pendingCalls++;
					return supplier.get().whenComplete(() -> {
						pendingCalls--;
						processBuffer();
					});
				}
				if (deque.size() > maxBufferedCalls) {
					throw new RejectedExecutionException("Too many operations running");
				}
				SettablePromise<T> result = new SettablePromise<>();
				deque.addLast(supplier);
				deque.addLast(result);
				return result;
			}
		};
	}

	public static AsyncExecutor retry(RetryPolicy<?> retryPolicy) {
		return new AsyncExecutor() {
			@Override
			public <T> Promise<T> execute(AsyncSupplier<T> supplier) {
				return Promises.retry(supplier, retryPolicy);
			}
		};
	}

	public static AsyncExecutor ofMaxRecursiveCalls(int maxRecursiveCalls) {
		checkArgument(maxRecursiveCalls >= 0, "Number of recursive calls cannot be less than 0");
		return new AsyncExecutor() {
			private final int maxCalls = maxRecursiveCalls + 1;
			private int counter = 0;

			@Override
			public <T> Promise<T> execute(AsyncSupplier<T> supplier) {
				Promise<T> promise = supplier.get();
				if (promise.isComplete()) {
					if (++counter % maxCalls == 0) {
						counter = 0;
						return promise.async();
					}
				} else {
					counter = 0;
				}
				return promise;
			}
		};
	}

}
