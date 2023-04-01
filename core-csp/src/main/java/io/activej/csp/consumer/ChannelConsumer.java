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

package io.activej.csp.consumer;

import io.activej.async.process.AsyncCloseable;
import io.activej.async.process.AsyncExecutor;
import io.activej.common.function.FunctionEx;
import io.activej.common.recycle.Recyclers;
import io.activej.csp.process.transformer.ChannelConsumerTransformer;
import io.activej.promise.Promise;
import io.activej.promise.SettableCallback;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static io.activej.common.exception.FatalErrorHandler.handleError;

/**
 * This interface represents consumer of data items that should be used serially
 * (each consecutive {@link #accept(Object)} operation should be called only after
 * previous {@link #accept(Object)} operation finishes)
 * <p>
 * After consumer is closed, all subsequent calls to {@link #accept(Object)} will
 * return a completed exceptionally promise.
 * <p>
 * If any exception is caught while consuming data items, {@link #closeEx(Exception)}
 * method should be called. All resources should be freed and the caught exception
 * should be propagated to all related processes.
 * <p>
 * If {@link #accept(Object)} takes {@code null} as argument, it represents end-of-stream
 * and means that no additional data should be consumed.
 */

public interface ChannelConsumer<T> extends AsyncCloseable {
	/**
	 * Consumes a provided value and returns a
	 * {@link Promise} as a marker of success.
	 */
	Promise<Void> accept(@Nullable T value);

	default Promise<Void> acceptEndOfStream() {
		return accept(null);
	}

	/**
	 * Accepts provided items and returns {@code Promise} as a
	 * marker of completion. If one of the items was accepted
	 * with an error, subsequent items will be recycled and a
	 * {@code Promise} of exception will be returned.
	 */
	@SuppressWarnings("unchecked")
	default Promise<Void> acceptAll(T... items) {
		return acceptAll(Arrays.asList(items));
	}

	/**
	 * Passes iterator's values to the {@code output} until it {@code hasNext()},
	 * then returns a promise of {@code null} as a marker of completion.
	 * <p>
	 * If there was an exception while accepting iterator, a promise of
	 * exception will be returned.
	 *
	 * @param it an {@link Iterator} which provides some values
	 * @return a promise of {@code null} as a marker of completion
	 */
	default Promise<Void> acceptAll(Iterator<? extends T> it) {
		if (!it.hasNext()) return Promise.complete();
		return Promise.ofCallback(cb -> acceptAllImpl(this, it, false, cb));
	}

	/**
	 * @see #acceptAll(Iterator)
	 */
	default Promise<Void> acceptAll(List<T> list) {
		if (list.isEmpty()) return Promise.complete();
		return Promise.ofCallback(cb -> acceptAllImpl(this, ((List<? extends T>) list).iterator(), true, cb));
	}

	/**
	 * Transforms current {@code ChannelConsumer} with provided {@link ChannelConsumerTransformer}.
	 *
	 * @param fn  transformer of the {@code ChannelConsumer}
	 * @param <R> result value after transformation
	 * @return result of transformation applied to the current {@code ChannelConsumer}
	 */
	default <R> R transformWith(ChannelConsumerTransformer<T, R> fn) {
		return fn.transform(this);
	}

	default ChannelConsumer<T> async() {
		return new AbstractChannelConsumer<>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				return ChannelConsumer.this.accept(value).async();
			}
		};
	}

	/**
	 * Creates a wrapper ChannelConsumer which executes current
	 * ChannelConsumer's {@code accept(T value)} within the
	 * {@code executor}.
	 *
	 * @param executor executes ChannelConsumer
	 * @return a wrapper of current ChannelConsumer which executes
	 * in provided {@code executor}
	 */
	default ChannelConsumer<T> withExecutor(AsyncExecutor executor) {
		return new AbstractChannelConsumer<>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				return executor.execute(() -> ChannelConsumer.this.accept(value));
			}
		};
	}

	/**
	 * Creates a wrapper ChannelConsumer - when its {@code accept(T value)}
	 * is called, if provided {@code value} doesn't equal {@code null}, it
	 * will be accepted by the provided {@code fn} first and then by this
	 * ChannelConsumer.
	 *
	 * @param fn {@link Consumer} which accepts the value passed by {@code apply(T value)}
	 * @return a wrapper ChannelConsumer
	 */
	default ChannelConsumer<T> peek(Consumer<? super T> fn) {
		return new AbstractChannelConsumer<>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				if (value != null) fn.accept(value);
				return ChannelConsumer.this.accept(value);
			}
		};
	}

	/**
	 * Creates a wrapper ChannelConsumer - when its {@code accept(T value)}
	 * is called, {@code fn} will be applied to the provided {@code value} first
	 * and the result of the {@code fn} will be accepted by current ChannelConsumer.
	 * If provide {@code value} is {@code null}, {@code fn} won't be applied.
	 *
	 * @param fn  {@link Function} to be applied to the value of {@code apply(T value)}
	 * @param <V> type of data accepted and returned by the {@code fn} and accepted by ChannelConsumer
	 * @return a wrapper ChannelConsumer
	 */
	default <V> ChannelConsumer<V> map(FunctionEx<? super V, ? extends T> fn) {
		return new AbstractChannelConsumer<>(this) {
			@Override
			protected Promise<Void> doAccept(V value) {
				if (value != null) {
					T newValue;
					try {
						newValue = fn.apply(value);
					} catch (Exception ex) {
						handleError(ex, fn);
						ChannelConsumer.this.closeEx(ex);
						return Promise.ofException(ex);
					}
					return ChannelConsumer.this.accept(newValue);
				} else {
					return ChannelConsumer.this.acceptEndOfStream();
				}
			}
		};
	}

	/**
	 * Creates a wrapper ChannelConsumer - when its {@code accept(T value)}
	 * is called, {@code fn} will be applied to the provided {@code value} first
	 * and the result of the {@code fn} will be accepted by current ChannelConsumer
	 * asynchronously. If provided {@code value} is {@code null}, {@code fn} won't
	 * be applied.
	 *
	 * @param fn  {@link Function} to be applied to the value of {@code apply(T value)}
	 * @param <V> type of data accepted by the {@code fn} and ChannelConsumer
	 * @return a wrapper ChannelConsumer
	 */
	default <V> ChannelConsumer<V> mapAsync(Function<? super V, Promise<T>> fn) {
		return new AbstractChannelConsumer<>(this) {
			@Override
			protected Promise<Void> doAccept(V value) {
				return value != null ?
						fn.apply(value)
								.then(ChannelConsumer.this::accept) :
						ChannelConsumer.this.acceptEndOfStream();
			}
		};
	}

	/**
	 * Creates a wrapper ChannelConsumer - when its {@code accept(T value)}
	 * is called, current ChannelConsumer will accept the value only of it
	 * passes {@link Predicate} test.
	 *
	 * @param predicate {@link Predicate} which is used to filter accepted value
	 * @return a wrapper ChannelConsumer
	 */
	default ChannelConsumer<T> filter(Predicate<? super T> predicate) {
		return new AbstractChannelConsumer<>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				if (value != null && predicate.test(value)) {
					return ChannelConsumer.this.accept(value);
				} else {
					Recyclers.recycle(value);
					return Promise.complete();
				}
			}
		};
	}

	/**
	 * Creates a wrapper ChannelConsumer - after its {@code accept(T value)}
	 * is called and completed, an acknowledgement is returned. An acknowledgement
	 * is a {@link SettablePromise} which is accepted by the provided {@code fn}
	 * and then materialized.
	 *
	 * @param fn a function applied to the {@code SettablePromise} which is then
	 *           materialized and returned
	 * @return a wrapper ChannelConsumer
	 */
	default ChannelConsumer<T> withAcknowledgement(UnaryOperator<Promise<Void>> fn) {
		SettablePromise<Void> acknowledgement = new SettablePromise<>();
		Promise<Void> newAcknowledgement = fn.apply(acknowledgement);
		return new AbstractChannelConsumer<>(this) {
			@Override
			protected Promise<Void> doAccept(@Nullable T value) {
				if (value != null) {
					return ChannelConsumer.this.accept(value)
							.then(Promise::of,
									e -> {
										acknowledgement.trySetException(e);
										return newAcknowledgement;
									});
				} else {
					ChannelConsumer.this.accept(null).run(acknowledgement::trySet);
					return newAcknowledgement;
				}
			}

			@Override
			protected void onClosed(Exception e) {
				acknowledgement.trySetException(e);
			}
		};
	}

	private static <T> void acceptAllImpl(ChannelConsumer<T> output, Iterator<? extends T> it, boolean ownership, SettableCallback<Void> cb) {
		while (it.hasNext()) {
			Promise<Void> accept = output.accept(it.next());
			if (accept.isResult()) continue;
			accept.run(($, e) -> {
				if (e == null) {
					acceptAllImpl(output, it, ownership, cb);
				} else {
					if (ownership) {
						it.forEachRemaining(Recyclers::recycle);
					} else {
						Recyclers.recycle(it);
					}
					cb.setException(e);
				}
			});
			return;
		}
		cb.set(null);
	}
}
