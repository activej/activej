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

import io.activej.async.exception.AsyncCloseException;
import io.activej.common.Checks;
import io.activej.common.recycle.Recyclers;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.ImplicitlyReactive;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

import static io.activej.reactor.Reactive.checkInReactorThread;

public final class ChannelBufferWithFallback<T> extends ImplicitlyReactive implements ChannelQueue<T> {
	private static final boolean CHECKS = Checks.isEnabled(ChannelBufferWithFallback.class);

	private final ChannelQueue<T> queue;
	private final Supplier<Promise<? extends ChannelQueue<T>>> bufferFactory;

	private @Nullable ChannelQueue<T> buffer;

	private @Nullable Exception exception;

	private SettablePromise<Void> waitingForBuffer;
	private boolean finished = false;

	public ChannelBufferWithFallback(ChannelQueue<T> queue, Supplier<Promise<? extends ChannelQueue<T>>> bufferFactory) {
		this.queue = queue;
		this.bufferFactory = bufferFactory;
	}

	@Override
	public Promise<Void> put(@Nullable T item) {
		if (CHECKS) checkInReactorThread(this);
		if (exception != null) {
			Recyclers.recycle(item);
			return Promise.ofException(exception);
		}
		return doPut(item);
	}

	@Override
	public Promise<T> take() {
		if (CHECKS) checkInReactorThread(this);
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

	private Promise<T> doTake() {
		if (CHECKS) checkInReactorThread(this);
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
				.then(Promise::of,
						e -> {
							if (!(e instanceof AsyncCloseException)) {
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
				.then((item, e) -> {
					if (e != null) {
						if (!(e instanceof AsyncCloseException)) {
							return Promise.ofException(e);
						}
					} else if (item != null) {
						return Promise.of(item);
					} else {
						// here item was null and we had no exception
						buffer.close();
					}
					// here either we had a close exception or item was null,
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
	public void closeEx(Exception e) {
		checkInReactorThread(this);
		if (exception != null) return;
		exception = e;
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

	public @Nullable Exception getException() {
		return exception;
	}
}
