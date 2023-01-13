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

import io.activej.common.Checks;
import io.activej.common.recycle.Recyclers;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.ImplicitlyReactive;
import org.jetbrains.annotations.Nullable;

import static io.activej.common.Checks.checkState;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Math.max;

/**
 * Represents a queue of elements which you can {@code put} and {@code take}.
 * In order to mark if an object is pending put or take to/from the queue,
 * there are corresponding {@code put} and {@code take} {@link SettablePromise}s.
 *
 * @param <T> the type of values that are stored in the buffer
 */
public final class ChannelBuffer<T> extends ImplicitlyReactive implements ChannelQueue<T> {
	private static final boolean CHECKS = Checks.isEnabled(ChannelBuffer.class);

	private Exception exception;

	private Object[] elements;
	private int tail;
	private int head;

	private final int bufferMinSize;
	private final int bufferMaxSize;

	private @Nullable SettablePromise<Void> put;
	private @Nullable SettablePromise<T> take;

	/**
	 * @see #ChannelBuffer(int, int)
	 */
	public ChannelBuffer(int bufferSize) {
		this(0, bufferSize);
	}

	/**
	 * Creates a ChannelBuffer with the buffer size of the next highest
	 * power of 2 (for example, if {@code bufferMinxSize = 113}, a buffer
	 * of 128 elements will be created).
	 *
	 * @param bufferMinSize a minimal amount of elements in the buffer
	 * @param bufferMaxSize a max amount of elements in the buffer
	 */
	public ChannelBuffer(int bufferMinSize, int bufferMaxSize) {
		this.bufferMinSize = bufferMinSize + 1;
		this.bufferMaxSize = bufferMaxSize;
		this.elements = new Object[1 << (32 - numberOfLeadingZeros(max(16, this.bufferMinSize) - 1))];
	}

	/**
	 * Checks if this buffer is saturated by comparing
	 * its current size with {@code bufferMaxSize}.
	 *
	 * @return {@code true} if this buffer size is greater
	 * than {@code bufferMaxSize}, otherwise returns {@code false}
	 */
	@Override
	public boolean isSaturated() {
		return size() > bufferMaxSize;
	}

	/**
	 * Checks if this buffer will be saturated if at
	 * least one more element will be added, by comparing
	 * its current size with {@code bufferMaxSize}.
	 *
	 * @return {@code true} if this buffer size is
	 * bigger or equal to the {@code bufferMaxSize},
	 * otherwise returns {@code false}
	 */
	public boolean willBeSaturated() {
		return size() >= bufferMaxSize;
	}

	/**
	 * Checks if this buffer has fewer
	 * elements than {@code bufferMinSize}.
	 *
	 * @return {@code true} if this buffer size is
	 * smaller than {@code bufferMinSize},
	 * otherwise returns {@code false}
	 */
	@Override
	public boolean isExhausted() {
		return size() < bufferMinSize;
	}

	/**
	 * Checks if this buffer will have fewer elements
	 * than {@code bufferMinSize}, if at least one
	 * more element will be taken, by comparing its
	 * current size with {@code bufferMinSize}.
	 *
	 * @return {@code true} if this buffer size is
	 * smaller or equal to the {@code bufferMinSize},
	 * otherwise returns {@code false}
	 */
	public boolean willBeExhausted() {
		return size() <= bufferMinSize;
	}

	/**
	 * Checks if this buffer contains elements.
	 *
	 * @return {@code true} if {@code tail}
	 * and {@code head} values are equal,
	 * otherwise {@code false}
	 */
	public boolean isEmpty() {
		return tail == head;
	}

	/**
	 * Returns amount of elements in this buffer.
	 */
	public int size() {
		return tail - head;
	}

	/**
	 * Adds provided item to the buffer and resets current {@code take}.
	 */
	public void add(@Nullable T item) throws Exception {
		if (CHECKS) checkInReactorThread(this);
		if (exception == null) {
			if (take != null) {
				assert isEmpty();
				SettablePromise<T> take = this.take;
				this.take = null;
				take.set(item);
				if (exception != null) throw exception;
				return;
			}

			doAdd(item);
		} else {
			Recyclers.recycle(item);
			throw exception;
		}
	}

	private void doAdd(@Nullable T value) {
		elements[(tail++) & (elements.length - 1)] = value;
	}

	/**
	 * Returns the head of the buffer if it is not empty,
	 * otherwise returns {@code null}. Increases the value of {@code head}.
	 * <p>
	 * If the buffer will have fewer elements than {@code bufferMinSize}
	 * after this poll and {@code put} promise is not {@code null},
	 * {@code put} will be set {@code null} after the poll.
	 * <p>
	 * If current {@code exception} is not {@code null},
	 * it will be thrown.
	 *
	 * @return head element of this buffer
	 * index if the buffer is not empty, otherwise {@code null}
	 * @throws Exception if current {@code exception}
	 *                   is not {@code null}
	 */
	public @Nullable T poll() throws Exception {
		if (CHECKS) checkInReactorThread(this);
		if (exception != null) throw exception;

		if (put != null && willBeExhausted()) {
			T item = doPoll();
			SettablePromise<Void> put = this.put;
			this.put = null;
			put.set(null);
			return item;
		}

		return !isEmpty() ? doPoll() : null;
	}

	private T doPoll() {
		assert head != tail;
		int pos = (head++) & (elements.length - 1);
		@SuppressWarnings("unchecked")
		T result = (T) elements[pos];
		elements[pos] = null;     // Must null out slot
		return result;
	}

	/**
	 * Puts {@code value} in this buffer and increases {@code tail} value.
	 * <p>
	 * Current {@code put} must be {@code null}. If
	 * current {@code exception} is not {@code null},
	 * a promise of this exception will be returned and
	 * the {@code value} will be recycled.
	 * <p>
	 * If this {@code take} is not {@code null}, the value
	 * will be set directly to the {@code set}, without
	 * adding to the buffer.
	 *
	 * @param item a value passed to the buffer
	 * @return promise of {@code null} or {@code exception}
	 * as a marker of completion
	 */
	@Override
	public Promise<Void> put(@Nullable T item) {
		if (CHECKS) {
			checkInReactorThread(this);
			checkState(put == null, "Previous put() has not finished yet");
		}
		if (exception == null) {
			if (take != null) {
				assert isEmpty();
				SettablePromise<T> take = this.take;
				this.take = null;
				take.set(item);
				return Promise.complete();
			}

			doAdd(item);

			if (isSaturated()) {
				put = new SettablePromise<>();
				return put;
			} else {
				return Promise.complete();
			}
		} else {
			Recyclers.recycle(item);
			return Promise.ofException(exception);
		}
	}

	/**
	 * Returns a promise of the head of
	 * the {@code buffer} if it is not empty.
	 * <p>
	 * If this buffer will be exhausted after this
	 * take and {@code put} promise is not {@code null},
	 * {@code put} will be set {@code null} after the poll.
	 * <p>
	 * Current {@code take} must be {@code null}. If
	 * current {@code exception} is not {@code null},
	 * a promise of this exception will be returned.
	 *
	 * @return promise of element taken from the buffer
	 */
	@Override
	public Promise<T> take() {
		if (CHECKS) {
			checkInReactorThread(this);
			checkState(take == null, "Previous take() has not finished yet");
		}
		if (exception == null) {
			if (put != null && willBeExhausted()) {
				assert !isEmpty();
				T item = doPoll();
				SettablePromise<Void> put = this.put;
				this.put = null;
				put.set(null);
				return Promise.of(item);
			}

			if (!isEmpty()) {
				return Promise.of(doPoll());
			}

			take = new SettablePromise<>();
			return take;
		} else {
			return Promise.ofException(exception);
		}
	}

	/**
	 * Closes the buffer if this {@code exception} is not
	 * {@code null}. Recycles all elements of the buffer and
	 * sets {@code elements}, {@code put} and {@code take} to
	 * {@code null}.
	 *
	 * @param e exception that is used to close buffer with
	 */
	@Override
	public void closeEx(Exception e) {
		checkInReactorThread(this);
		if (exception != null) return;
		exception = e;
		if (put != null) {
			put.setException(e);
			put = null;
		}
		if (take != null) {
			take.setException(e);
			take = null;
		}
		for (int i = head; i != tail; i = (i + 1) & (elements.length - 1)) {
			Recyclers.recycle(elements[i]);
		}
		//noinspection AssignmentToNull - resource release
		elements = null;
	}

	public @Nullable Exception getException() {
		return exception;
	}
}
