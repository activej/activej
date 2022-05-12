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

import io.activej.async.process.AsyncCloseable;
import io.activej.csp.AbstractChannelConsumer;
import io.activej.csp.AbstractChannelSupplier;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.ChannelTransformer;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a queue of elements, which you can {@code put}
 * or {@code take} to pass from {@link ChannelConsumer} to
 * {@link ChannelSupplier}.
 *
 * @param <T> type of values stored in the queue
 */
public interface ChannelQueue<T> extends ChannelTransformer<T, T>, AsyncCloseable {
	/**
	 * Puts an item in the queue and returns a
	 * {@code promise} of {@code null} as a marker of completion.
	 *
	 * @param item a item passed to the queue
	 * @return {@code promise} of {@code null} as a marker of completion
	 */
	Promise<Void> put(@Nullable T item);

	/**
	 * Takes an element of this queue and wraps it in {@code promise}.
	 *
	 * @return a {@code promise} of value from the queue
	 */
	Promise<T> take();

	boolean isSaturated();

	boolean isExhausted();

	/**
	 * Returns a {@code ChannelConsumer} which puts value in
	 * this queue when {@code accept(T value)} is called.
	 *
	 * @return a {@code ChannelConsumer} for this queue
	 */
	default ChannelConsumer<T> getConsumer() {
		return new AbstractChannelConsumer<>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				return put(value);
			}
		};
	}

	/**
	 * Returns a {@code ChannelConsumer} which puts non-null
	 * value in this queue when {@code accept(T value)} is
	 * called. Otherwise, puts {@code null} and waits for the
	 * {@code acknowledgement} completion.
	 * <p>
	 * This method is useful if you need to control the situations
	 * when there are no more elements to be accepted (for example,
	 * get a {@code ChannelSupplier} in such case).
	 *
	 * @param acknowledgement a promise which will work when
	 *                        a {@code null} value is passed
	 * @return a ChannelConsumer with custom behaviour in case a
	 * {@code null} value is accepted
	 */
	default ChannelConsumer<T> getConsumer(Promise<Void> acknowledgement) {
		return new AbstractChannelConsumer<>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				if (value != null) return put(value);
				return put(null).both(acknowledgement);
			}
		};
	}

	/**
	 * Returns a {@link ChannelSupplier} which gets value from this
	 * queue wrapped in {@code Promise} when {@code get()} is called.
	 *
	 * @return a ChannelSupplier which takes values from this queue
	 */
	default ChannelSupplier<T> getSupplier() {
		return new AbstractChannelSupplier<>(this) {
			@Override
			protected Promise<T> doGet() {
				return take();
			}
		};
	}

	@Override
	default ChannelConsumer<T> transform(ChannelConsumer<T> consumer) {
		Promise<Void> stream = getSupplier().streamTo(consumer);
		return getConsumer().withAcknowledgement(ack -> ack.both(stream));
	}

	@Override
	default ChannelSupplier<T> transform(ChannelSupplier<T> supplier) {
		Promise<Void> stream = supplier.streamTo(getConsumer());
		return getSupplier().withEndOfStream(eos -> eos.both(stream));
	}
}
