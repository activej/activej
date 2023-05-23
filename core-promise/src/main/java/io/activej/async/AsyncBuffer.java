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

import io.activej.async.function.AsyncFunction;
import io.activej.common.function.FunctionEx;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

public final class AsyncBuffer<A, R> {
	private final AsyncFunction<A, R> executor;
	private final Supplier<A> bufferSupplier;

	private int activeCalls;
	private A buffer;
	private SettablePromise<R> bufferedPromise;

	public AsyncBuffer(AsyncFunction<A, R> executor, Supplier<A> bufferSupplier) {
		this.executor = executor;
		this.bufferSupplier = bufferSupplier;
	}

	public <V> Promise<R> add(BiConsumer<A, V> argumentAccumulator, V argument) {
		if (bufferedPromise == null) {
			this.buffer = bufferSupplier.get();
			this.bufferedPromise = new SettablePromise<>();
		}
		argumentAccumulator.accept(buffer, argument);
		return bufferedPromise;
	}

	public <V, T> Promise<T> add(BiConsumer<A, V> argumentAccumulator, FunctionEx<R, T> resultExtractor, V argument) {
		return add(argumentAccumulator, argument).map(resultExtractor);
	}

	public Promise<Void> flush() {
		if (bufferedPromise == null) return Promise.complete();
		A buffer = this.buffer;
		SettablePromise<R> bufferedPromise = this.bufferedPromise;
		this.buffer = null;
		this.bufferedPromise = null;
		this.activeCalls++;
		return executor.apply(buffer)
			.whenComplete((v, e) -> {
				activeCalls--;
				bufferedPromise.trySet(v, e);
			})
			.toVoid();
	}

	public boolean isActive() {
		return activeCalls != 0;
	}

	public int getActiveCalls() {
		return activeCalls;
	}

	public boolean isBuffered() {
		return bufferedPromise != null;
	}

	public A getBuffer() {
		return buffer;
	}

	public Promise<R> getBufferedPromise() {
		return bufferedPromise;
	}

	public void cancelBufferedPromise(Exception e) {
		if (bufferedPromise != null) {
			SettablePromise<R> bufferedPromise = this.bufferedPromise;
			this.bufferedPromise = null;
			this.buffer = null;
			bufferedPromise.trySetException(e);
		}
	}

}
