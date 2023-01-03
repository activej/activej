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

package io.activej.csp;

import io.activej.async.function.AsyncSupplier;
import io.activej.async.process.AsyncCloseable;
import io.activej.async.process.AsyncExecutor;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.function.BiConsumerEx;
import io.activej.common.function.FunctionEx;
import io.activej.common.recycle.Recyclers;
import io.activej.csp.dsl.ChannelSupplierTransformer;
import io.activej.csp.queue.ChannelQueue;
import io.activej.net.socket.tcp.TcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.activej.common.exception.FatalErrorHandlers.handleError;
import static io.activej.reactor.Reactor.getCurrentReactor;

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
	 * @see #of(AsyncSupplier, AsyncCloseable)
	 */
	static <T> ChannelSupplier<T> of(AsyncSupplier<T> supplier) {
		return of(supplier, null);
	}

	/**
	 * Wraps {@link AsyncSupplier} in ChannelSupplier, when {@code get()}
	 * is called, {@code AsyncSupplier}'s {@code get()} will be executed.
	 *
	 * @param supplier  an {@code AsyncSupplier} to be wrapped in ChannelSupplier
	 * @param closeable a {@code Cancellable} which will be set
	 *                  for the ChannelSupplier wrapper
	 * @param <T>       data type wrapped in {@code AsyncSupplier} and ChannelSupplier
	 * @return ChannelSupplier which wraps {@code AsyncSupplier}
	 */
	static <T> ChannelSupplier<T> of(AsyncSupplier<T> supplier, @Nullable AsyncCloseable closeable) {
		return new AbstractChannelSupplier<>(closeable) {
			@Override
			protected Promise<T> doGet() {
				return supplier.get();
			}
		};
	}

	/**
	 * Returns a ChannelSupplier received from {@link ChannelQueue}.
	 */
	static <T> ChannelSupplier<T> ofConsumer(Consumer<ChannelConsumer<T>> consumer, ChannelQueue<T> queue) {
		consumer.accept(queue.getConsumer());
		return queue.getSupplier();
	}

	/**
	 * Wraps provided default {@link Supplier} to ChannelSupplier.
	 */
	static <T> ChannelSupplier<T> ofSupplier(Supplier<Promise<T>> supplier) {
		return of(supplier::get);
	}

	/**
	 * Returns a {@link ChannelSuppliers.ChannelSupplierEmpty}.
	 */
	static <T> ChannelSupplier<T> of() {
		return new ChannelSuppliers.ChannelSupplierEmpty<>();
	}

	/**
	 * Wraps provided {@code value} to a {@link ChannelSuppliers.ChannelSupplierOfValue}.
	 *
	 * @param value a value to be wrapped in ChannelSupplier
	 * @return a {@code ChannelSupplierOfValue} which wraps the {@code value}
	 */
	static <T> ChannelSupplier<T> of(T value) {
		return new ChannelSuppliers.ChannelSupplierOfValue<>(value);
	}

	/**
	 * @see #ofIterator(Iterator)
	 */
	@SafeVarargs
	static <T> ChannelSupplier<T> of(T... values) {
		return ofList(List.of(values));
	}

	/**
	 * Returns a {@link ChannelSuppliers.ChannelSupplierOfException}
	 * of provided exception.
	 *
	 * @param e a {@link Exception} to be wrapped in ChannelSupplier
	 */
	static <T> ChannelSupplier<T> ofException(Exception e) {
		return new ChannelSuppliers.ChannelSupplierOfException<>(e);
	}

	/**
	 * @see #ofIterator(Iterator)
	 */
	static <T> ChannelSupplier<T> ofList(List<? extends T> list) {
		return new ChannelSuppliers.ChannelSupplierOfIterator<>(list.iterator(), true);
	}

	/**
	 * @see #ofIterator(Iterator)
	 */
	static <T> ChannelSupplier<T> ofStream(Stream<? extends T> stream) {
		return ofIterator(stream.iterator());
	}

	/**
	 * Wraps provided {@code Iterator} into
	 * {@link ChannelSuppliers.ChannelSupplierOfIterator}.
	 *
	 * @param iterator an iterator to be wrapped in ChannelSupplier
	 * @return a ChannelSupplier which wraps elements of <T> type
	 */
	static <T> ChannelSupplier<T> ofIterator(Iterator<? extends T> iterator) {
		return new ChannelSuppliers.ChannelSupplierOfIterator<>(iterator, false);
	}

	/**
	 * Wraps {@link TcpSocket#read()} operation into {@link ChannelSupplier}
	 *
	 * @return {@link ChannelSupplier} of ByteBufs that are read from network
	 */
	static ChannelSupplier<ByteBuf> ofSocket(TcpSocket socket) {
		return ChannelSuppliers.prefetch(ChannelSupplier.of(socket::read, socket));
	}

	/**
	 * Wraps {@code promise} of ChannelSupplier in ChannelSupplier or
	 * returns the ChannelSupplier from {@code promise} itself.
	 * <p>
	 * If {@code promise} is completed, it will be materialized and its result
	 * (a ChannelSupplier) will be returned.
	 * <p>
	 * Otherwise, when {@code get()} is called, it will wait until {@code promise}
	 * completes and {@code promise} result's (a ChannelSupplier) {@code get()}
	 * operation will be executed. If the {@code promise} completes exceptionally,
	 * a {@code promise} of exception will be returned.
	 *
	 * @param promise wraps a {@code ChannelSupplier}
	 * @return a ChannelSupplier of {@code promise} or a wrapper ChannelSupplier
	 */
	static <T> ChannelSupplier<T> ofPromise(Promise<? extends ChannelSupplier<T>> promise) {
		if (promise.isResult()) return promise.getResult();
		return new AbstractChannelSupplier<>() {
			ChannelSupplier<T> supplier;
			Exception exception;

			@Override
			protected Promise<T> doGet() {
				if (supplier != null) return supplier.get();
				return promise.then(supplier -> {
					this.supplier = supplier;
					return supplier.get();
				});
			}

			@Override
			protected void onClosed(Exception e) {
				exception = e;
				promise.whenResult(supplier -> supplier.closeEx(e));
			}
		};
	}

	static <T> ChannelSupplier<T> ofAnotherReactor(Reactor anotherReactor, ChannelSupplier<T> anotherReactorSupplier) {
		if (getCurrentReactor() == anotherReactor) {
			return anotherReactorSupplier;
		}
		return new AbstractChannelSupplier<>() {
			@Override
			protected Promise<T> doGet() {
				SettablePromise<T> promise = new SettablePromise<>();
				reactor.startExternalTask();
				anotherReactor.execute(() ->
						anotherReactorSupplier.get()
								.run((item, e) -> {
									reactor.execute(() -> promise.accept(item, e));
									reactor.completeExternalTask();
								}));
				return promise;
			}

			@Override
			protected void onClosed(Exception e) {
				reactor.startExternalTask();
				anotherReactor.execute(() -> {
					anotherReactorSupplier.closeEx(e);
					reactor.completeExternalTask();
				});
			}
		};
	}

	/**
	 * Returns a {@code ChannelSupplier} wrapped in {@link Supplier}
	 * and calls its {@code get()} when {@code get()} method is called.
	 *
	 * @param provider a provider of {@code ChannelSupplier}
	 * @return a {@code ChannelSupplier} that was wrapped in
	 * the {@code provider}
	 */
	static <T> ChannelSupplier<T> ofLazyProvider(Supplier<? extends ChannelSupplier<T>> provider) {
		return new AbstractChannelSupplier<>() {
			private ChannelSupplier<T> supplier;

			@Override
			protected Promise<T> doGet() {
				if (supplier == null) supplier = provider.get();
				return supplier.get();
			}

			@Override
			protected void onClosed(Exception e) {
				if (supplier != null) {
					supplier.closeEx(e);
				}
			}
		};
	}

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
	 * executed by the provided {@code asyncExecutor}.
	 */
	default ChannelSupplier<T> withExecutor(AsyncExecutor asyncExecutor) {
		return new AbstractChannelSupplier<>(this) {
			@Override
			protected Promise<T> doGet() {
				return asyncExecutor.execute(ChannelSupplier.this::get);
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
						.mapIfNonNull(value -> {
							try {
								return fn.apply(value);
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
						.thenIfNonNull(fn::apply);
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
							.thenIf(value -> value != null && !predicate.test(value),
									value -> {
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
						.mapIfNonNull(value -> {
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
	 * @see ChannelSuppliers#streamTo(ChannelSupplier, ChannelConsumer)
	 */
	default Promise<Void> streamTo(ChannelConsumer<T> consumer) {
		return ChannelSuppliers.streamTo(this, consumer);
	}

	default Promise<Void> streamTo(Promise<? extends ChannelConsumer<T>> consumer) {
		return ChannelSuppliers.streamTo(this, ChannelConsumer.ofPromise(consumer));
	}

	/**
	 * Binds this ChannelSupplier to provided {@link ChannelInput}
	 */
	default Promise<Void> bindTo(ChannelInput<T> to) {
		return to.set(this);
	}

	/**
	 * @see ChannelSuppliers#collect
	 */
	default <A, R> Promise<R> toCollector(Collector<T, A, R> collector) {
		return ChannelSuppliers.collect(this,
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

}
