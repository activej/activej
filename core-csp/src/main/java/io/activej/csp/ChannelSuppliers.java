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
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.MemSize;
import io.activej.common.collection.Try;
import io.activej.common.function.BiConsumerEx;
import io.activej.common.function.FunctionEx;
import io.activej.common.recycle.Recyclers;
import io.activej.csp.queue.ChannelBuffer;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import static io.activej.common.Utils.iteratorOf;
import static io.activej.common.Utils.nullify;
import static io.activej.common.exception.FatalErrorHandlers.handleError;
import static java.lang.Math.min;
import static java.util.Arrays.asList;

/**
 * Provides additional functionality for managing {@link ChannelSupplier}s.
 * Includes helper classes: ChannelSupplierOfException, ChannelSupplierOfIterator,
 * ChannelSupplierOfValue, ChannelSupplierEmpty.
 */
public final class ChannelSuppliers {

	/**
	 * @see #concat(Iterator)
	 */
	public static <T> ChannelSupplier<T> concat(ChannelSupplier<? extends T> supplier1, ChannelSupplier<? extends T> supplier2) {
		return concat(asList(supplier1, supplier2));
	}

	/**
	 * @see #concat(Iterator)
	 */
	@SafeVarargs
	public static <T> ChannelSupplier<T> concat(ChannelSupplier<? extends T>... suppliers) {
		return concat(asList(suppliers));
	}

	/**
	 * @see #concat(Iterator)
	 */
	public static <T> ChannelSupplier<T> concat(List<ChannelSupplier<? extends T>> suppliers) {
		return new ChannelSupplierConcat<>(suppliers.iterator(), true);
	}

	/**
	 * Creates a new ChannelSupplier which on {@code get()} call returns
	 * the result wrapped in {@code promise} of the first ChannelSuppliers'
	 * {@code promise} that was successfully completed with a non-null result.
	 * If all the ChannelSuppliers of the iterator have a {@code null}
	 * {@code promise} result, a {@code promise} of {@code null} will be returned.
	 * <p>
	 * If one of the ChannelSuppliers' {@code promises} completes with an exception,
	 * all subsequent elements of the iterator will be closed and a
	 * {@code promise} of exception will be returned.
	 *
	 * @param iterator an iterator of ChannelSuppliers
	 * @param <T>      type of data wrapped in the ChannelSuppliers
	 * @return a ChannelSupplier of {@code <T>}
	 */
	public static <T> ChannelSupplier<T> concat(Iterator<? extends ChannelSupplier<? extends T>> iterator) {
		return new ChannelSupplierConcat<>(iterator, false);
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
	public static <T, A, R> Promise<R> collect(ChannelSupplier<T> supplier,
			A initialValue, BiConsumerEx<A, T> accumulator, FunctionEx<A, R> finisher) {
		return Promise.ofCallback(cb ->
				toCollectorImpl(supplier, initialValue, accumulator, finisher, cb));
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

	public static <T> Promise<Void> streamTo(Promise<ChannelSupplier<T>> supplier, Promise<ChannelConsumer<T>> consumer) {
		return Promises.toTuple(supplier.toTry(), consumer.toTry())
				.then(t -> streamTo(t.getValue1(), t.getValue2()));
	}

	public static <T> Promise<Void> streamTo(Try<ChannelSupplier<T>> supplier, Try<ChannelConsumer<T>> consumer) {
		if (supplier.isSuccess() && consumer.isSuccess()) {
			return streamTo(supplier.get(), consumer.get());
		}
		Exception exception = new Exception("Channel stream failed");
		supplier.consume(AsyncCloseable::close, exception::addSuppressed);
		consumer.consume(AsyncCloseable::close, exception::addSuppressed);
		return Promise.ofException(exception);
	}

	/**
	 * Streams data from the {@code supplier} to the {@code consumer} until {@code get()}
	 * of {@code supplier} returns a promise of {@code null}.
	 * <p>
	 * If {@code get} returns a promise of exception or there was an exception while
	 * {@code consumer} accepted values, a promise of {@code exception} will be
	 * returned and the process will stop.
	 *
	 * @param supplier a supplier which provides some data
	 * @param consumer a consumer which accepts the provided by supplier data
	 * @param <T>      a data type of values passed from the supplier to consumer
	 * @return a promise of {@code null} as a marker of completion of stream,
	 * or promise of exception, if there was an exception while streaming
	 */
	public static <T> Promise<Void> streamTo(ChannelSupplier<T> supplier, ChannelConsumer<T> consumer) {
		return Promise.ofCallback(cb ->
				streamToImpl(supplier, consumer, cb));
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

	public static <T> ChannelSupplier<T> prefetch(int count, ChannelSupplier<T> actual) {
		ChannelBuffer<T> buffer = new ChannelBuffer<>(count);
		actual.streamTo(buffer.getConsumer());
		return buffer.getSupplier();
	}

	public static <T> ChannelSupplier<T> prefetch(ChannelSupplier<T> actual) {
		ChannelZeroBuffer<T> buffer = new ChannelZeroBuffer<>();
		actual.streamTo(buffer.getConsumer());
		return buffer.getSupplier();
	}

	/**
	 * Transforms this {@code ChannelSupplier} data of <T> type with provided {@code fn},
	 * which returns an {@link Iterator} of a <V> type. Then provides this value to ChannelSupplier of <V>.
	 */
	public static <T, V> ChannelSupplier<V> remap(ChannelSupplier<T> supplier, Function<? super T, ? extends Iterator<? extends V>> fn) {
		return new AbstractChannelSupplier<V>(supplier) {
			Iterator<? extends V> iterator = iteratorOf();
			boolean endOfStream;

			@Override
			protected Promise<V> doGet() {
				if (iterator.hasNext()) return Promise.of(iterator.next());
				return Promise.ofCallback(this::next);
			}

			private void next(SettablePromise<V> cb) {
				if (!endOfStream) {
					supplier.get()
							.run((item, e) -> {
								if (e == null) {
									if (item == null) endOfStream = true;
									iterator = fn.apply(item);
									if (iterator.hasNext()) {
										cb.set(iterator.next());
									} else {
										next(cb);
									}
								} else {
									cb.setException(e);
								}
							});
				} else {
					cb.set(null);
				}
			}
		};
	}

	private static final @NotNull MemSize DEFAULT_BUFFER_SIZE = MemSize.kilobytes(8);

	/**
	 * @see #inputStreamAsChannelSupplier(Executor, int, InputStream)
	 */
	public static ChannelSupplier<ByteBuf> inputStreamAsChannelSupplier(Executor executor, MemSize bufSize, InputStream is) {
		return inputStreamAsChannelSupplier(executor, bufSize.toInt(), is);
	}

	/**
	 * Creates an asynchronous {@link ChannelSupplier} out of some {@link InputStream}.
	 * <p>
	 * Uses a default buffer size of <b>8 kilobytes</b>
	 *
	 * @see #inputStreamAsChannelSupplier(Executor, int, InputStream)
	 */
	public static ChannelSupplier<ByteBuf> inputStreamAsChannelSupplier(Executor executor, InputStream is) {
		return inputStreamAsChannelSupplier(executor, DEFAULT_BUFFER_SIZE, is);
	}

	/**
	 * Creates an asynchronous {@link ChannelSupplier<ByteBuf>} out of some {@link InputStream}.
	 * <p>
	 * I/O operations are executed using a specified {@link Executor}, so that the channel supplier
	 * operations does not block the eventloop thread.
	 * <p>
	 * A size of a {@link ByteBuf} returned by {@link ChannelSupplier#get()} will not exceed specified limit
	 * <p>
	 * Passed {@link InputStream} will be closed once a resulting {@link ChannelSupplier<ByteBuf>} is closed or
	 * in case an error occurs during channel supplier operations.
	 * <p>
	 * <b>This method should be called from within eventloop thread</b>
	 *
	 * @param executor    an executor that will execute blocking I/O
	 * @param bufSize     a limit on a size of a byte buf supplied by returned {@link ChannelSupplier<ByteBuf>}
	 * @param inputStream an {@link InputStream} that is transformed into a {@link ChannelSupplier<ByteBuf>}
	 * @return a {@link ChannelSupplier<ByteBuf>} out ouf an {@link InputStream}
	 */
	public static ChannelSupplier<ByteBuf> inputStreamAsChannelSupplier(Executor executor, int bufSize, InputStream inputStream) {
		return new AbstractChannelSupplier<ByteBuf>() {
			@Override
			protected Promise<ByteBuf> doGet() {
				return Promise.ofBlocking(executor, () -> {
					ByteBuf buf = ByteBufPool.allocate(bufSize);
					int readBytes;
					try {
						readBytes = inputStream.read(buf.array(), 0, bufSize);
					} catch (IOException e) {
						buf.recycle();
						throw e;
					}
					if (readBytes != -1) {
						buf.moveTail(readBytes);
						return buf;
					} else {
						buf.recycle();
						return null;
					}
				});
			}

			@Override
			protected void onClosed(@NotNull Exception e) {
				executor.execute(() -> {
					try {
						inputStream.close();
					} catch (IOException ignored) {
					}
				});
			}
		};
	}

	/**
	 * Creates an {@link InputStream} out of a {@link ChannelSupplier<ByteBuf>}.
	 * <p>
	 * Asynchronous operations are executed in a context of a specified {@link Eventloop}
	 * <p>
	 * Passed {@link ChannelSupplier<ByteBuf>} will be closed once a resulting {@link InputStream} is closed or
	 * in case an error occurs while reading data.
	 * <p>
	 * <b>{@link InputStream}'s methods are blocking, so they should not be called from an eventloop thread</b>
	 *
	 * @param eventloop       an eventloop that will execute asynchronous operations
	 * @param channelSupplier a {@link ChannelSupplier<ByteBuf>} that is transformed to an {@link InputStream}
	 * @return an {@link InputStream} out ouf a {@link ChannelSupplier<ByteBuf>}
	 */
	public static InputStream channelSupplierAsInputStream(Eventloop eventloop, ChannelSupplier<ByteBuf> channelSupplier) {
		return new InputStream() {
			private @Nullable ByteBuf current = null;
			private boolean isClosed;
			private boolean isEOS;

			@Override
			public int read() throws IOException {
				return doRead(ByteBuf::readByte);
			}

			@Override
			public int read(byte @NotNull [] b, int off, int len) throws IOException {
				return doRead(buf -> buf.read(b, off, min(buf.readRemaining(), len)));
			}

			private int doRead(ToIntFunction<ByteBuf> reader) throws IOException {
				if (isClosed) {
					throw new IOException("Stream Closed");
				}
				if (isEOS) return -1;
				ByteBuf peeked = current;
				if (peeked == null) {
					ByteBuf buf;
					do {
						buf = submit(channelSupplier::get);
						if (buf == null) {
							isEOS = true;
							return -1;
						}
					} while (!buf.canRead());
					peeked = buf;
				}
				int result = reader.applyAsInt(peeked);
				if (peeked.canRead()) {
					current = peeked;
				} else {
					current = null;
					peeked.recycle();
				}
				return result;
			}

			@Override
			public void close() throws IOException {
				if (isClosed) return;
				isClosed = true;
				current = nullify(current, ByteBuf::recycle);
				submit(() -> {
					channelSupplier.close();
					return Promise.complete();
				});
			}

			private <T> T submit(AsyncSupplier<T> supplier) throws IOException {
				CompletableFuture<T> future = eventloop.submit(supplier::get);
				try {
					return future.get();
				} catch (InterruptedException e) {
					close();
					Thread.currentThread().interrupt();
					throw new IOException(e);
				} catch (ExecutionException e) {
					close();
					Throwable cause = e.getCause();
					if (cause instanceof IOException) throw (IOException) cause;
					if (cause instanceof RuntimeException) throw (RuntimeException) cause;
					if (cause instanceof Exception) throw new IOException(cause);
					if (cause instanceof Error) throw (Error) cause;
					throw new RuntimeException(cause);
				}
			}

		};
	}

	/**
	 * Represents a {@code ChannelSupplier} which always returns
	 * a promise of {@code null}.
	 */
	public static class ChannelSupplierEmpty<T> extends AbstractChannelSupplier<T> {
		@Override
		protected Promise<T> doGet() {
			return Promise.of(null);
		}
	}

	/**
	 * Represents a {@code ChannelSupplier} of one value. Returns a promise of the value when
	 * {@code get} is called for the first time, all subsequent calls will return {@code null}.
	 */
	public static final class ChannelSupplierOfValue<T> extends AbstractChannelSupplier<T> {
		private T item;

		public T getValue() {
			return item;
		}

		public T takeValue() {
			T item = this.item;
			this.item = null;
			return item;
		}

		public ChannelSupplierOfValue(@NotNull T item) {
			this.item = item;
		}

		@Override
		protected Promise<T> doGet() {
			T item = takeValue();
			return Promise.of(item);
		}

		@Override
		protected void onCleanup() {
			item = nullify(item, Recyclers::recycle);
		}
	}

	/**
	 * Represents a {@code ChannelSupplier} which wraps the provided iterator and
	 * returns promises of iterator's values until {@code hasNext()} is true, when
	 * there are no more values left, a promise of {@code null} is returned.
	 */
	public static final class ChannelSupplierOfIterator<T> extends AbstractChannelSupplier<T> {
		private final Iterator<? extends T> iterator;
		private final boolean ownership;

		public ChannelSupplierOfIterator(Iterator<? extends T> iterator, boolean ownership) {
			this.iterator = iterator;
			this.ownership = ownership;
		}

		@Override
		protected Promise<T> doGet() {
			return Promise.of(iterator.hasNext() ? iterator.next() : null);
		}

		@Override
		protected void onCleanup() {
			if (ownership) {
				iterator.forEachRemaining(Recyclers::recycle);
			} else {
				Recyclers.recycle(iterator);
			}
		}
	}

	/**
	 * Represents a {@code ChannelSupplier} which always returns a promise of exception.
	 */
	public static final class ChannelSupplierOfException<T> extends AbstractChannelSupplier<T> {
		private final Exception e;

		public ChannelSupplierOfException(Exception e) {
			this.e = e;
		}

		@Override
		protected Promise<T> doGet() {
			return Promise.ofException(e);
		}
	}

	public static final class ChannelSupplierConcat<T> extends AbstractChannelSupplier<T> {
		private final Iterator<? extends ChannelSupplier<? extends T>> iterator;
		private final boolean ownership;

		public ChannelSupplierConcat(Iterator<? extends ChannelSupplier<? extends T>> iterator, boolean ownership) {
			this.iterator = iterator;
			this.ownership = ownership;
		}

		ChannelSupplier<? extends T> current = ChannelSupplier.of();

		@Override
		protected Promise<T> doGet() {
			return current.get()
					.then((value, e) -> {
						if (e == null) {
							if (value != null) {
								return Promise.of(value);
							} else {
								if (iterator.hasNext()) {
									current = iterator.next();
									return get();
								} else {
									return Promise.of(null);
								}
							}
						} else {
							closeEx(e);
							return Promise.ofException(e);
						}
					});
		}

		@Override
		protected void onClosed(@NotNull Exception e) {
			current.closeEx(e);
			if (ownership) {
				iterator.forEachRemaining(Recyclers::recycle);
			} else {
				Recyclers.recycle(iterator);
			}
		}
	}
}
