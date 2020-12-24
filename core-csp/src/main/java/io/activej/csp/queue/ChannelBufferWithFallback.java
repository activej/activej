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

package io.activej.csp.queue;

import io.activej.common.exception.CloseException;
import io.activej.common.recycle.Recyclers;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

public final class ChannelBufferWithFallback<T> implements ChannelQueue<T> {
	private final ChannelQueue<T> queue;
	private final Supplier<Promise<? extends ChannelQueue<T>>> bufferFactory;

	@Nullable
	private ChannelQueue<T> buffer;

	@Nullable
	private Exception exception;

	private SettablePromise<Void> waitingForBuffer;
	private boolean finished = false;

	public ChannelBufferWithFallback(ChannelQueue<T> queue, Supplier<Promise<? extends ChannelQueue<T>>> bufferFactory) {
		this.queue = queue;
		this.bufferFactory = bufferFactory;
	}

	@Override
	public Promise<Void> put(@Nullable T item) {
		if (exception != null) {
			Recyclers.recycle(item);
			return Promise.ofException(exception);
		}
		return doPut(item);
	}

	@Override
	public Promise<T> take() {
		if (exception != null) {
			return Promise.ofException(exception);
		}
		return doTake();
	}

	private Promise<Void> doPut(@Nullable T item) {
		if (item == null) {
			finished = true;
		}
		if (buffer != null) {
			return secondaryPut(item);
		}
		if (waitingForBuffer != null) {
			// no buffer, *yet*
			return waitingForBuffer.then($ -> secondaryPut(item));
		}
		// try to push into primary
		if (!queue.isSaturated()) {
			return queue.put(item);
		}
		// primary is saturated, creating secondary buffer
		SettablePromise<Void> waitingForBuffer = new SettablePromise<>();
		this.waitingForBuffer = waitingForBuffer;
		return bufferFactory.get()
				.then(buffer -> {
					this.buffer = buffer;
					waitingForBuffer.set(null);
					this.waitingForBuffer = null;
					return secondaryPut(item);
				});
	}

	public Promise<T> doTake() {
		if (buffer != null) {
			return secondaryTake();
		}
		if (waitingForBuffer != null) {
			return waitingForBuffer.then($ -> secondaryTake());
		}
		// we already received null and have no items left
		if (finished && queue.isExhausted()) {
			return Promise.of(null);
		}
		return queue.take();
	}

	private Promise<Void> secondaryPut(@Nullable T item) {
		assert buffer != null;
		return buffer.put(item)
				.thenEx(($, e) -> {
					if (e == null) {
						return Promise.complete();
					}
					if (!(e instanceof CloseException)) {
						return Promise.ofException(e);
					}
					// buffer was already closed for whatever reason,
					// retry the whole thing (may cause loops, but should not)
					buffer = null;
					return doPut(item);
				});
	}

	private Promise<T> secondaryTake() {
		if (buffer == null) {
			return doTake();
		}
		return buffer.take()
				.thenEx((item, e) -> {
					if (e != null) {
						if (!(e instanceof CloseException)) {
							return Promise.ofException(e);
						}
					} else if (item != null) {
						return Promise.of(item);
					} else {
						// here item was null and we had no exception
						buffer.close();
					}
					// here either we had a close exception or item was null
					// so we retry the whole thing (same as in secondaryPut)
					buffer = null;
					return doTake();
				});
	}

	@Override
	public boolean isSaturated() {
		return queue.isSaturated() && buffer != null && buffer.isSaturated();
	}

	@Override
	public boolean isExhausted() {
		return queue.isExhausted() && (buffer == null || buffer.isExhausted());
	}

	@Override
	public void closeEx(@NotNull Throwable e) {
		if (exception != null) return;
		exception = e instanceof Exception ? (Exception) e : new RuntimeException(e);
		queue.closeEx(e);
		if (waitingForBuffer != null) {
			waitingForBuffer.whenResult(() -> {
				assert buffer != null;
				buffer.closeEx(e);
			});
		}
		if (buffer != null) {
			buffer.closeEx(e);
		}
	}

	@Nullable
	public Throwable getException() {
		return exception;
	}
}
