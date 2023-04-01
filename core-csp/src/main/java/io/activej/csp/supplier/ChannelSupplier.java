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

package io.activej.csp.supplier;

import io.activej.async.process.AsyncCloseable;
import io.activej.async.process.AsyncExecutor;
import io.activej.common.collection.Try;
import io.activej.common.function.BiConsumerEx;
import io.activej.common.function.FunctionEx;
import io.activej.common.recycle.Recyclers;
import io.activej.csp.ChannelInput;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.process.transformer.ChannelSupplierTransformer;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;

import java.util.List;
import java.util.Objects;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static io.activej.common.exception.FatalErrorHandler.handleError;

/**
 * This interface represents supplier of {@link Promise} of data that should be used serially
 * (each consecutive {@link #get()}) operation should be called only after previous
 * {@link #get()} operation finishes.
 * <p>
 * After supplier is closed, all subsequent calls to {@link #get()} will return promise,
 * completed exceptionally.
 * <p>
 * If any exception is caught while supplying data items, {@link #closeEx(Exception)} method
 * should be called. All resources should be freed and the caught exception should be
 * propagated to all related processes.
 * <p>
 * If {@link #get()} returns {@link Promise} of {@code null}, it represents end-of-stream
 * and means that no additional data should be queried.
 */
public interface ChannelSupplier<T> extends AsyncCloseable {
	Promise<T> get();

	/**
	 * Transforms this ChannelSupplier with the provided {@code fn}.
	 *
	 * @param <R> returned result after transformation
	 * @param fn  {@link ChannelSupplierTransformer} applied to the ChannelSupplier
	 */
	default <R> R transformWith(ChannelSupplierTransformer<T, R> fn) {
		return fn.transform(this);
	}

	/**
	 * Creates and returns a new {@link AbstractChannelSupplier}
	 * based on current ChannelSupplier and makes its promise
	 * complete asynchronously.
	 */
	default ChannelSupplier<T> async() {
		return new AbstractChannelSupplier<>(this) {
			@Override
			protected Promise<T> doGet() {
				return ChannelSupplier.this.get().async();
			}
		};
	}

	/**
	 * Creates and returns a new {@link AbstractChannelSupplier}
	 * based on current ChannelSupplier and makes its promise
	 * executed by the provided {@code executor}.
	 */
	default ChannelSupplier<T> withExecutor(AsyncExecutor executor) {
		return new AbstractChannelSupplier<>(this) {
			@Override
			protected Promise<T> doGet() {
				return executor.execute(ChannelSupplier.this::get);
			}
		};
	}

	/**
	 * Creates and returns a new {@link AbstractChannelSupplier}
	 * based on current ChannelSupplier and when its Promise completes
	 * successfully, the result is accepted by the provided {@code fn}.
	 */
	default ChannelSupplier<T> peek(Consumer<? super T> fn) {
		return new AbstractChannelSupplier<>(this) {
			@Override
			protected Promise<T> doGet() {
				return ChannelSupplier.this.get()
						.whenResult(Objects::nonNull, fn::accept);
			}
		};
	}

	/**
	 * Creates and returns a new {@link AbstractChannelSupplier}
	 * based on current ChannelSupplier and when its Promise completes,
	 * applies provided {@code fn} to the result.
	 */
	default <V> ChannelSupplier<V> map(FunctionEx<? super T, ? extends V> fn) {
		return new AbstractChannelSupplier<>(this) {
			@Override
			protected Promise<V> doGet() {
				return ChannelSupplier.this.get()
						.map(t -> {
							if (t == null) return null;
							try {
								return fn.apply(t);
							} catch (Exception ex) {
								handleError(ex, fn);
								ChannelSupplier.this.closeEx(ex);
								throw ex;
							}
						});
			}
		};
	}

	/**
	 * Creates and returns a new {@link AbstractChannelSupplier}
	 * based on current ChannelSupplier and applies provided {@code fn}
	 * to its Promise asynchronously.
	 */
	default <V> ChannelSupplier<V> mapAsync(Function<? super T, Promise<V>> fn) {
		return new AbstractChannelSupplier<>(this) {
			@Override
			protected Promise<V> doGet() {
				return ChannelSupplier.this.get()
						.then(value -> value != null ? fn.apply(value) : Promise.of(null));
			}
		};
	}

	/**
	 * Creates and returns a new {@link AbstractChannelSupplier}
	 * based on current ChannelSupplier and checks if its Promise's value(s)
	 * match(es) the predicate, leaving only those value(s) which pass the test.
	 */
	default ChannelSupplier<T> filter(Predicate<? super T> predicate) {
		return new AbstractChannelSupplier<>(this) {
			@Override
			protected Promise<T> doGet() {
				while (true) {
					Promise<T> promise = ChannelSupplier.this.get();
					if (promise.isResult()) {
						T value = promise.getResult();
						if (value == null || predicate.test(value)) return promise;
						Recyclers.recycle(value);
						continue;
					}
					return promise
							.then(value -> {
								if (value == null || predicate.test(value)) return Promise.of(value);
								Recyclers.recycle(value);
								return get();
							});
				}
			}
		};
	}

	/**
	 * Creates and returns a new {@link AbstractChannelSupplier} based on current
	 * ChannelSupplier, when its {@code get} is called, its values will be returned
	 * until they don't fit the {@code predicate}. If one of the results passed the
	 * {@code predicate test}, consequent {@code get} operations will return {@code null}.
	 */
	default ChannelSupplier<T> until(Predicate<? super T> predicate) {
		return new AbstractChannelSupplier<>(this) {
			boolean stop = false;

			@Override
			protected Promise<T> doGet() {
				if (stop) {
					return Promise.of(null);
				}
				return ChannelSupplier.this.get()
						.map(value -> {
							if (value == null) return null;
							if (predicate.test(value)) {
								stop = true;
							}
							return value;
						});
			}
		};
	}

	/**
	 * Creates and returns a new {@link AbstractChannelSupplier}
	 * based on current ChannelSupplier. Even if its Promise completes
	 * with an exception, {@code get()} method will return a successfully
	 * completed Promise (in case of exception, with {@code null} result value).
	 */
	default ChannelSupplier<T> lenient() {
		return new AbstractChannelSupplier<>(this) {
			@Override
			protected Promise<T> doGet() {
				return ChannelSupplier.this.get()
						.map((value, e) -> value);
			}
		};
	}

	/**
	 * Streams data from this {@link ChannelSupplier} to the {@link ChannelConsumer} until {@link #get()}
	 * returns a promise of {@code null}.
	 * <p>
	 * If {@link #get()} returns a promise of exception or there was an exception while
	 * {@link  ChannelConsumer} accepted values, a promise of {@code exception} will be
	 * returned and the process will stop.
	 *
	 * @param consumer a consumer which accepts the provided by supplier data
	 * @return a promise of {@code null} as a marker of completion of stream,
	 * or promise of exception, if there was an exception while streaming
	 */
	default Promise<Void> streamTo(ChannelConsumer<T> consumer) {
		return Promise.ofCallback(cb -> streamToImpl(this, consumer, cb));
	}

	default Promise<Void> streamTo(Promise<? extends ChannelConsumer<T>> consumer) {
		return streamTo(ChannelConsumers.ofPromise(consumer));
	}

	/**
	 * Binds this ChannelSupplier to provided {@link ChannelInput}
	 */
	default Promise<Void> bindTo(ChannelInput<T> to) {
		return to.set(this);
	}

	/**
	 * @see ChannelSupplier#collect
	 */
	default <A, R> Promise<R> toCollector(Collector<T, A, R> collector) {
		return collect(this,
				collector.supplier().get(), BiConsumerEx.of(collector.accumulator()), FunctionEx.of(collector.finisher()));
	}

	/**
	 * @see #toCollector(Collector)
	 */
	default Promise<List<T>> toList() {
		return toCollector(Collectors.toList());
	}

	default ChannelSupplier<T> withEndOfStream(UnaryOperator<Promise<Void>> fn) {
		SettablePromise<Void> endOfStream = new SettablePromise<>();
		Promise<Void> newEndOfStream = fn.apply(endOfStream);
		return new AbstractChannelSupplier<>(this) {
			@SuppressWarnings("unchecked")
			@Override
			protected Promise<T> doGet() {
				return ChannelSupplier.this.get()
						.then((item, e) -> {
							if (e == null) {
								if (item != null) return Promise.of(item);
								endOfStream.trySet(null);
							} else {
								endOfStream.trySetException(e);
							}
							return (Promise<T>) newEndOfStream;
						});
			}

			@Override
			protected void onClosed(Exception e) {
				endOfStream.trySetException(e);
			}
		};
	}

	/**
	 * Collects data provided by the {@code supplier} asynchronously and returns a
	 * promise of accumulated result. This process will be getting values from the
	 * {@code supplier}, until a promise of {@code null} is returned, which represents
	 * end of stream.
	 * <p>
	 * If {@code get} returns a promise of exception or there was an exception while
	 * {@code accumulator} accepted values, a promise of {@code exception} will be
	 * returned and the process will stop.
	 *
	 * @param supplier     a {@code ChannelSupplier} which provides data to be collected
	 * @param initialValue a value which will accumulate the results of accumulator
	 * @param accumulator  a {@link BiConsumer} which may perform some operations over provided
	 *                     by supplier data and accumulates the result to the initialValue
	 * @param finisher     a {@link Function} which performs the final transformation of the
	 *                     accumulated value
	 * @param <T>          a data type provided by the {@code supplier}
	 * @param <A>          an intermediate accumulation data type
	 * @param <R>          a data type of final result of {@code finisher}
	 * @return a promise of accumulated result, transformed by the {@code finisher}
	 */
	static <T, A, R> Promise<R> collect(ChannelSupplier<T> supplier,
			A initialValue, BiConsumerEx<A, T> accumulator, FunctionEx<A, R> finisher) {
		return Promise.ofCallback(cb ->
				toCollectorImpl(supplier, initialValue, accumulator, finisher, cb));
	}

	static <T> Promise<Void> streamTo(Promise<ChannelSupplier<T>> supplier, Promise<ChannelConsumer<T>> consumer) {
		return Promises.toTuple(supplier.toTry(), consumer.toTry())
				.then(t -> streamTo(t.value1(), t.value2()));
	}

	static <T> Promise<Void> streamTo(Try<ChannelSupplier<T>> supplier, Try<ChannelConsumer<T>> consumer) {
		if (supplier.isSuccess() && consumer.isSuccess()) {
			return supplier.get().streamTo(consumer.get());
		}
		Exception exception = new Exception("Channel stream failed");
		supplier.consume(AsyncCloseable::close, exception::addSuppressed);
		consumer.consume(AsyncCloseable::close, exception::addSuppressed);
		return Promise.ofException(exception);
	}

	private static <T> void streamToImpl(ChannelSupplier<T> supplier, ChannelConsumer<T> consumer, SettablePromise<Void> cb) {
		Promise<T> supplierPromise;
		while (true) {
			supplierPromise = supplier.get();
			if (!supplierPromise.isResult()) break;
			T item = supplierPromise.getResult();
			if (item == null) break;
			Promise<Void> consumerPromise = consumer.accept(item);
			if (consumerPromise.isResult()) continue;
			consumerPromise.run(($, e) -> {
				if (e == null) {
					streamToImpl(supplier, consumer, cb);
				} else {
					supplier.closeEx(e);
					cb.trySetException(e);
				}
			});
			return;
		}
		supplierPromise
				.run((item, e1) -> {
					if (e1 == null) {
						consumer.accept(item)
								.run(($, e2) -> {
									if (e2 == null) {
										if (item != null) {
											streamToImpl(supplier, consumer, cb);
										} else {
											cb.trySet(null);
										}
									} else {
										supplier.closeEx(e2);
										cb.trySetException(e2);
									}
								});
					} else {
						consumer.closeEx(e1);
						cb.trySetException(e1);
					}
				});
	}

	private static <T, A, R> void toCollectorImpl(ChannelSupplier<T> supplier,
			A accumulatedValue, BiConsumerEx<A, T> accumulator, FunctionEx<A, R> finisher,
			SettablePromise<R> cb) {
		Promise<T> promise;
		while (true) {
			promise = supplier.get();
			if (!promise.isResult()) break;
			T item = promise.getResult();
			if (item != null) {
				try {
					accumulator.accept(accumulatedValue, item);
				} catch (Exception ex) {
					handleError(ex, cb);
					supplier.closeEx(ex);
					cb.setException(ex);
					return;
				}
				continue;
			}
			break;
		}
		promise.run((value, e) -> {
			if (e == null) {
				if (value != null) {
					try {
						accumulator.accept(accumulatedValue, value);
					} catch (Exception ex) {
						handleError(ex, cb);
						supplier.closeEx(ex);
						cb.setException(ex);
						return;
					}
					toCollectorImpl(supplier, accumulatedValue, accumulator, finisher, cb);
				} else {
					R result;
					try {
						result = finisher.apply(accumulatedValue);
					} catch (Exception ex) {
						handleError(ex, cb);
						cb.setException(ex);
						return;
					}
					cb.set(result);
				}
			} else {
				Recyclers.recycle(accumulatedValue);
				cb.setException(e);
			}
		});
	}
}
