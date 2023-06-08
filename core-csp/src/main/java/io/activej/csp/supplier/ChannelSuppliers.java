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

import io.activej.async.function.AsyncSupplier;
import io.activej.async.process.AsyncCloseable;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.MemSize;
import io.activej.common.annotation.StaticFactories;
import io.activej.common.function.SupplierEx;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.csp.queue.ChannelBuffer;
import io.activej.csp.queue.ChannelQueue;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.csp.supplier.impl.*;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.activej.reactor.Reactor.getCurrentReactor;

/**
 * Provides additional functionality for managing {@link ChannelSupplier}s.
 * Includes helper classes: ChannelSupplierOfException, ChannelSupplierOfIterator,
 * ChannelSupplierOfValue, ChannelSupplierEmpty.
 */
@StaticFactories(ChannelSupplier.class)
public class ChannelSuppliers {

	/**
	 * @see #ofAsyncSupplier(AsyncSupplier, AsyncCloseable)
	 */
	public static <T> ChannelSupplier<T> ofAsyncSupplier(AsyncSupplier<T> supplier) {
		return ofAsyncSupplier(supplier, null);
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
	public static <T> ChannelSupplier<T> ofAsyncSupplier(AsyncSupplier<T> supplier, @Nullable AsyncCloseable closeable) {
		return new OfAsyncSupplier<>(supplier, closeable);
	}

	/**
	 * Wraps provided default {@link Supplier} to ChannelSupplier.
	 */
	public static <T> ChannelSupplier<T> ofSupplier(SupplierEx<T> supplier) {
		return ofSupplier(supplier, null);
	}

	/**
	 * Wraps provided default {@link Supplier} to ChannelSupplier.
	 */
	public static <T> ChannelSupplier<T> ofSupplier(SupplierEx<T> supplier, @Nullable AsyncCloseable closeable) {
		return new OfSupplier<>(supplier, closeable);
	}

	/**
	 * Creates a ChannelSupplier received from {@link ChannelQueue}.
	 */
	public static <T> ChannelSupplier<T> ofConsumer(Consumer<ChannelConsumer<T>> consumer, ChannelQueue<T> queue) {
		consumer.accept(queue.getConsumer());
		return queue.getSupplier();
	}

	/**
	 * Creates a {@code ChannelSupplier} which is empty (always returns
	 * a promise of {@code null})
	 */
	public static <T> ChannelSupplier<T> empty() {
		return new Empty<>();
	}

	/**
	 * Creates a {@link ChannelSupplier} of one value. Returns a promise of the value when
	 * {@code get} is called for the first time, all subsequent calls will return {@code null}.
	 *
	 * @param value a value to be wrapped in ChannelSupplier
	 * @return a {@link ChannelSupplier} which wraps the {@code value}
	 */
	public static <T> ChannelSupplier<T> ofValue(T value) {
		return new OfValue<>(value);
	}

	/**
	 * @see #empty()
	 */
	public static <T> ChannelSupplier<T> ofValues() {
		return empty();
	}

	/**
	 * @see #ofValue(T)
	 */
	public static <T> ChannelSupplier<T> ofValues(T value) {
		return ofValue(value);
	}

	/**
	 * @see #ofIterator(Iterator)
	 */
	@SafeVarargs
	public static <T> ChannelSupplier<T> ofValues(T... values) {
		return ofList(List.of(values));
	}

	/**
	 * Creates a {@code ChannelSupplier} which always returns a promise of exception.
	 */
	public static <T> ChannelSupplier<T> ofException(Exception e) {
		return new OfException<>(e);
	}

	/**
	 * @see #ofIterator(Iterator)
	 */
	public static <T> ChannelSupplier<T> ofList(List<? extends T> list) {
		return new OfIterator<>(list.iterator(), true);
	}

	/**
	 * @see #ofIterator(Iterator)
	 */
	public static <T> ChannelSupplier<T> ofStream(Stream<? extends T> stream) {
		return ofIterator(stream.iterator());
	}

	/**
	 * Creates a {@link ChannelSupplier} which wraps the provided iterator and
	 * returns promises of iterator's values until {@code hasNext()} is true, when
	 * there are no more values left, a promise of {@code null} is returned.
	 *
	 * @param iterator an iterator to be wrapped in ChannelSupplier
	 * @return a ChannelSupplier which wraps elements of <T> type
	 */
	public static <T> ChannelSupplier<T> ofIterator(Iterator<? extends T> iterator) {
		return new OfIterator<>(iterator, false);
	}

	/**
	 * Wraps {@link ITcpSocket#read()} operation into {@link ChannelSupplier}
	 *
	 * @return {@link ChannelSupplier} of ByteBufs that are read from network
	 */
	public static ChannelSupplier<ByteBuf> ofSocket(ITcpSocket socket) {
		return ChannelSuppliers.prefetch(ChannelSuppliers.ofAsyncSupplier(socket::read, socket));
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
	public static <T> ChannelSupplier<T> ofPromise(Promise<? extends ChannelSupplier<T>> promise) {
		if (promise.isResult()) return promise.getResult();
		return new OfPromise<>(promise);
	}

	public static <T> ChannelSupplier<T> ofAnotherReactor(Reactor anotherReactor, ChannelSupplier<T> anotherReactorSupplier) {
		if (getCurrentReactor() == anotherReactor) {
			return anotherReactorSupplier;
		}
		return new OfAnotherReactor<>(anotherReactor, anotherReactorSupplier);
	}

	/**
	 * Creates a {@code ChannelSupplier} wrapped in {@link Supplier}
	 * and calls its {@code get()} when {@code get()} method is called.
	 *
	 * @param provider a provider of {@code ChannelSupplier}
	 * @return a {@code ChannelSupplier} that was wrapped in
	 * the {@code provider}
	 */
	public static <T> ChannelSupplier<T> ofLazyProvider(Supplier<? extends ChannelSupplier<T>> provider) {
		return new OfLazyProvider<>(provider);
	}

	/**
	 * @see #concat(Iterator)
	 */
	public static <T> ChannelSupplier<T> concat(ChannelSupplier<? extends T> supplier1, ChannelSupplier<? extends T> supplier2) {
		return concat(List.of(supplier1, supplier2));
	}

	/**
	 * @see #concat(Iterator)
	 */
	@SafeVarargs
	public static <T> ChannelSupplier<T> concat(ChannelSupplier<? extends T>... suppliers) {
		return concat(List.of(suppliers));
	}

	/**
	 * @see #concat(Iterator)
	 */
	public static <T> ChannelSupplier<T> concat(List<ChannelSupplier<? extends T>> suppliers) {
		return new Concat<>(suppliers.iterator(), true);
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
		return new Concat<>(iterator, false);
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
		return new Remap<>(supplier, fn);
	}

	private static final MemSize DEFAULT_BUFFER_SIZE = MemSize.kilobytes(8);

	/**
	 * @see #ofInputStream(Executor, int, InputStream)
	 */
	public static ChannelSupplier<ByteBuf> ofInputStream(Executor executor, MemSize bufSize, InputStream is) {
		return ofInputStream(executor, bufSize.toInt(), is);
	}

	/**
	 * Creates an asynchronous {@link ChannelSupplier} out of some {@link InputStream}.
	 * <p>
	 * Uses a default buffer size of <b>8 kilobytes</b>
	 *
	 * @see #ofInputStream(Executor, int, InputStream)
	 */
	public static ChannelSupplier<ByteBuf> ofInputStream(Executor executor, InputStream is) {
		return ofInputStream(executor, DEFAULT_BUFFER_SIZE, is);
	}

	/**
	 * Creates an asynchronous {@link ChannelSupplier<ByteBuf>} out of some {@link InputStream}.
	 * <p>
	 * I/O operations are executed using a specified {@link Executor}, so that the channel supplier
	 * operations does not block the reactor.
	 * <p>
	 * A size of a {@link ByteBuf} returned by {@link ChannelSupplier#get()} will not exceed specified limit
	 * <p>
	 * Passed {@link InputStream} will be closed once a resulting {@link ChannelSupplier<ByteBuf>} is closed or
	 * in case an error occurs during channel supplier operations.
	 * <p>
	 * <b>This method should be called from within reactor</b>
	 *
	 * @param executor    an executor that will execute blocking I/O
	 * @param bufSize     a limit on a size of a byte buf supplied by returned {@link ChannelSupplier<ByteBuf>}
	 * @param inputStream an {@link InputStream} that is transformed into a {@link ChannelSupplier<ByteBuf>}
	 * @return a {@link ChannelSupplier<ByteBuf>} out ouf an {@link InputStream}
	 */
	public static ChannelSupplier<ByteBuf> ofInputStream(Executor executor, int bufSize, InputStream inputStream) {
		return new OfInputStream(executor, bufSize, inputStream);
	}

}
