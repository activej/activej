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

package io.activej.promise;

import io.activej.async.AsyncAccumulator;
import io.activej.async.AsyncBuffer;
import io.activej.async.exception.AsyncTimeoutException;
import io.activej.async.function.AsyncFunction;
import io.activej.async.function.AsyncRunnable;
import io.activej.async.function.AsyncSupplier;
import io.activej.common.function.BiConsumerEx;
import io.activej.common.function.FunctionEx;
import io.activej.common.recycle.Recyclers;
import io.activej.common.tuple.*;
import io.activej.reactor.Reactor;
import io.activej.reactor.schedule.ScheduledRunnable;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Array;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static io.activej.common.Utils.*;
import static io.activej.common.exception.FatalErrorHandlers.handleError;
import static io.activej.promise.PromisePredicates.isResult;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static io.activej.reactor.util.RunnableWithContext.wrapContext;
import static java.util.Arrays.asList;

/**
 * Allows managing multiple {@link Promise}s.
 */
@SuppressWarnings({"WeakerAccess", "unchecked"})
public final class Promises {
	/**
	 * @see #timeout(long, Promise)
	 */
	@Contract(pure = true)
	public static <T> Promise<T> timeout(Duration delay, Promise<T> promise) {
		return timeout(delay.toMillis(), promise);
	}

	/**
	 * Waits until the delay passes and if the {@code Promise} is still
	 * not complete, tries to complete it with {@code TIMEOUT_EXCEPTION}.
	 *
	 * @param delay   time of delay
	 * @param promise the Promise to be tracked
	 * @return {@code Promise}
	 */
	@Contract(pure = true)
	public static <T> Promise<T> timeout(long delay, Promise<T> promise) {
		if (promise.isComplete()) return promise;
		if (delay <= 0) return Promise.ofException(new AsyncTimeoutException("Promise timeout"));
		return promise.next(new NextPromise<>() {
			@Nullable ScheduledRunnable schedule = getCurrentReactor().delay(delay,
					wrapContext(this, () -> {
						promise.whenResult(Recyclers::recycle);
						schedule = null;
						tryCompleteExceptionally(new AsyncTimeoutException("Promise timeout"));
					}));

			@Override
			public void accept(T result, @Nullable Exception e) {
				schedule = nullify(schedule, ScheduledRunnable::cancel);
				if (e == null) {
					tryComplete(result);
				} else {
					tryCompleteExceptionally(e);
				}
			}
		});
	}

	@Contract(pure = true)
	public static Promise<Void> delay(Duration delay) {
		return delay(delay.toMillis(), (Void) null);
	}

	@Contract(pure = true)
	public static Promise<Void> delay(long delayMillis) {
		return delay(delayMillis, (Void) null);
	}

	@Contract(pure = true)
	public static <T> Promise<T> delay(Duration delay, T value) {
		return delay(delay.toMillis(), value);
	}

	@Contract(pure = true)
	public static <T> Promise<T> delay(long delayMillis, T value) {
		if (delayMillis <= 0) return Promise.of(value);
		SettablePromise<T> cb = new SettablePromise<>();
		getCurrentReactor().delay(delayMillis, wrapContext(cb, () -> cb.set(value)));
		return cb;
	}

	/**
	 * @see #delay(long, Promise)
	 */
	@Contract(pure = true)
	public static <T> Promise<T> delay(Duration delay, Promise<T> promise) {
		return delay(delay.toMillis(), promise);
	}

	/**
	 * Delays completion of provided {@code promise} for
	 * the defined period of time.
	 *
	 * @param delayMillis delay in millis
	 * @param promise     the {@code Promise} to be delayed
	 * @return completed {@code Promise}
	 */
	@Contract(pure = true)
	public static <T> Promise<T> delay(long delayMillis, Promise<T> promise) {
		if (delayMillis <= 0) return promise;
		return Promise.ofCallback(cb ->
				getCurrentReactor().delay(delayMillis, wrapContext(cb, () -> promise.run(cb))));
	}

	@Contract(pure = true)
	public static <T> Promise<T> interval(Duration interval, Promise<T> promise) {
		return interval(interval.toMillis(), promise);
	}

	@Contract(pure = true)
	public static <T> Promise<T> interval(long intervalMillis, Promise<T> promise) {
		return intervalMillis <= 0 ?
				promise :
				promise.then(value -> Promise.ofCallback(cb -> getCurrentReactor().delay(intervalMillis, wrapContext(cb, () -> cb.set(value)))));
	}

	/**
	 * @see #schedule(Promise, long)
	 */
	@Contract(pure = true)
	public static Promise<Void> schedule(Instant instant) {
		return schedule((Void) null, instant.toEpochMilli());
	}

	/**
	 * @see #schedule(Promise, long)
	 */
	@Contract(pure = true)
	public static Promise<Void> schedule(long timestamp) {
		return schedule((Void) null, timestamp);
	}

	/**
	 * @see #schedule(Promise, long)
	 */
	@Contract(pure = true)
	public static <T> Promise<T> schedule(T value, Instant instant) {
		return schedule(value, instant.toEpochMilli());
	}

	/**
	 * @see #schedule(Promise, long)
	 */
	@Contract(pure = true)
	public static <T> Promise<T> schedule(T value, long timestamp) {
		SettablePromise<T> cb = new SettablePromise<>();
		getCurrentReactor().schedule(timestamp, wrapContext(cb, () -> cb.set(value)));
		return cb;
	}

	/**
	 * @see #schedule(Promise, long)
	 */
	@Contract(pure = true)
	public static <T> Promise<T> schedule(Promise<T> promise, Instant instant) {
		return schedule(promise, instant.toEpochMilli());
	}

	/**
	 * Schedules completion of the {@code Promise} so that it will
	 * be completed after the timestamp even if its operations
	 * were completed earlier.
	 */
	@Contract(pure = true)
	public static <T> Promise<T> schedule(Promise<T> promise, long timestamp) {
		return Promise.ofCallback(cb ->
				getCurrentReactor().schedule(timestamp, wrapContext(cb, () -> promise.run(cb))));
	}

	/**
	 * @see Promises#all(List)
	 */
	@Contract(pure = true)
	public static Promise<Void> all() {
		return Promise.complete();
	}

	/**
	 * @see Promises#all(List)
	 */
	@Contract(pure = true)
	public static Promise<Void> all(Promise<?> promise1) {
		return promise1.toVoid();
	}

	/**
	 * Optimized for 2 promises.
	 *
	 * @see Promises#all(List)
	 */
	@Contract(pure = true)
	public static Promise<Void> all(Promise<?> promise1, Promise<?> promise2) {
		return promise1.both(promise2);
	}

	/**
	 * @see Promises#all(List)
	 */
	@Contract(pure = true)
	public static Promise<Void> all(Promise<?>... promises) {
		return all(List.of(promises));
	}

	/**
	 * Returns a {@code Promise} that completes when
	 * all of the {@code promises} are completed.
	 */
	@Contract(pure = true)
	public static Promise<Void> all(List<? extends Promise<?>> promises) {
		int size = promises.size();
		if (size == 0) return Promise.complete();
		if (size == 1) return promises.get(0).map(AbstractPromise::recycleToVoid);
		if (size == 2) return promises.get(0).both(promises.get(1));

		return allIterator(promises.iterator(), true);
	}

	/**
	 * @see Promises#all(List)
	 */
	@Contract(pure = true)
	public static Promise<Void> all(Stream<? extends Promise<?>> promises) {
		return all(promises.iterator());
	}

	/**
	 * Returns {@code Promise} that completes when all of the {@code promises}
	 * are completed. If at least one of the {@code promises} completes
	 * exceptionally, a {@link CompleteExceptionallyPromise} will be returned.
	 */
	public static Promise<Void> all(Iterator<? extends Promise<?>> promises) {
		return allIterator(promises, false);
	}

	private static Promise<Void> allIterator(Iterator<? extends Promise<?>> promises, boolean ownership) {
		if (!promises.hasNext()) return all();
		PromiseAll<Object> resultPromise = new PromiseAll<>();
		while (promises.hasNext()) {
			Promise<?> promise = promises.next();
			if (promise.isResult()) {
				Recyclers.recycle(promise.getResult());
				continue;
			}
			if (promise.isException()) {
				if (ownership) {
					promises.forEachRemaining(Recyclers::recycle);
				} else {
					Recyclers.recycle(promises);
				}
				return Promise.ofException(promise.getException());
			}
			resultPromise.countdown++;
			promise.run(resultPromise);
		}
		resultPromise.countdown--;
		return resultPromise.countdown == 0 ? Promise.complete() : resultPromise;
	}

	/**
	 * Returns a {@link CompleteExceptionallyPromise} with {@link Exception},
	 * since this method doesn't accept any {@code Promise}s
	 *
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	public static <T> Promise<T> any() {
		return Promise.ofException(new Exception("There are no promises to be complete"));
	}

	/**
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	public static <T> Promise<T> any(Promise<? extends T> promise1) {
		return (Promise<T>) promise1;
	}

	/**
	 * Optimized for 2 promises.
	 *
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	public static <T> Promise<T> any(Promise<? extends T> promise1, Promise<? extends T> promise2) {
		return ((Promise<T>) promise1).either(promise2);
	}

	/**
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	@SafeVarargs
	public static <T> Promise<T> any(Promise<? extends T>... promises) {
		return any(isResult(), List.of(promises));
	}

	/**
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	public static <T> Promise<T> any(List<? extends Promise<? extends T>> promises) {
		int size = promises.size();
		if (size == 0) return any();
		if (size == 1) return (Promise<T>) promises.get(0);
		if (size == 2) return ((Promise<T>) promises.get(0)).either(promises.get(1));
		return any(isResult(), promises);
	}

	/**
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	public static <T> Promise<T> any(Stream<? extends Promise<? extends T>> promises) {
		return any(isResult(), promises.iterator());
	}

	public static <T> Promise<T> any(Iterator<? extends Promise<? extends T>> promises) {
		return any(isResult(), promises);
	}

	@Contract(pure = true)
	public static <T> Promise<T> any(BiPredicate<? super T, Exception> predicate, Promise<? extends T> promise1) {
		return any(predicate, List.of(promise1));
	}

	@Contract(pure = true)
	public static <T> Promise<T> any(BiPredicate<? super T, Exception> predicate, Promise<? extends T> promise1, Promise<? extends T> promise2) {
		return any(predicate, List.of(promise1, promise2));
	}

	@Contract(pure = true)
	@SafeVarargs
	public static <T> Promise<T> any(BiPredicate<? super T, Exception> predicate, Promise<? extends T>... promises) {
		return any(predicate, List.of(promises));
	}

	@Contract(pure = true)
	public static <T> Promise<T> any(BiPredicate<? super T, Exception> predicate, Stream<? extends Promise<? extends T>> promises) {
		return any(predicate, promises.iterator());
	}

	@Contract(pure = true)
	public static <T> Promise<T> any(BiPredicate<? super T, Exception> predicate, List<? extends Promise<? extends T>> promises) {
		return anyIterator(predicate, promises.iterator(), true);
	}

	public static <T> Promise<T> any(BiPredicate<? super T, Exception> predicate, Iterator<? extends Promise<? extends T>> promises) {
		return anyIterator(predicate, promises, false);
	}

	private static <T> Promise<T> anyIterator(BiPredicate<? super T, Exception> predicate,
			Iterator<? extends Promise<? extends T>> promises, boolean ownership) {
		if (!promises.hasNext()) return any();
		PromiseAny<T> resultPromise = new PromiseAny<>(predicate);
		while (promises.hasNext()) {
			Promise<? extends T> promise = promises.next();
			if (promise.isComplete()) {
				T result = promise.getResult();
				if (predicate.test(result, promise.getException())) {
					if (ownership) {
						promises.forEachRemaining(Recyclers::recycle);
					} else {
						Recyclers.recycle(promises);
					}
					return Promise.of(result);
				}
				Recyclers.recycle(result);
				continue;
			}
			resultPromise.countdown++;
			promise.run(resultPromise);
		}
		resultPromise.countdown--;
		return resultPromise.countdown == 0 ? any() : resultPromise;
	}

	/**
	 * Returns a successfully completed {@code Promise}
	 * with an empty list as the result.
	 */
	@Contract(pure = true)
	public static <T> Promise<List<T>> toList() {
		return Promise.of(List.of());
	}

	/**
	 * Returns a completed {@code Promise}
	 * with a result wrapped in {@code List}.
	 */
	@Contract(pure = true)
	public static <T> Promise<List<T>> toList(Promise<? extends T> promise1) {
		return promise1.map(t -> List.of(t));
	}

	/**
	 * Returns {@code Promise} with a list of {@code promise1} and {@code promise2} results.
	 */
	@Contract(pure = true)
	public static <T> Promise<List<T>> toList(Promise<? extends T> promise1, Promise<? extends T> promise2) {
		return promise1.combine(promise2, List::of);
	}

	/**
	 * @see Promises#toList(List)
	 */
	@Contract(pure = true)
	@SafeVarargs
	public static <T> Promise<List<T>> toList(Promise<? extends T>... promises) {
		return toList(List.of(promises));
	}

	/**
	 * Reduces list of {@code Promise}s into Promise&lt;List&gt;.
	 */
	@Contract(pure = true)
	public static <T> Promise<List<T>> toList(List<? extends Promise<? extends T>> promises) {
		int size = promises.size();
		if (size == 0) return Promise.of(List.of());
		if (size == 1) return promises.get(0).map(t -> List.of(t));
		if (size == 2) return promises.get(0).combine(promises.get(1), List::of);
		return toListImpl(promises.iterator(), promises.size(), true);
	}

	/**
	 * @see Promises#toList(List)
	 */
	@Contract(pure = true)
	public static <T> Promise<List<T>> toList(Stream<? extends Promise<? extends T>> promises) {
		return toList(promises.iterator());
	}

	/**
	 * @see Promises#toList(List)
	 */
	@Contract(pure = true)
	public static <T> Promise<List<T>> toList(Iterable<? extends Promise<? extends T>> promises) {
		return toList(promises.iterator());
	}

	/**
	 * @see Promises#toList(List)
	 */
	@Contract(pure = true)
	public static <T> Promise<List<T>> toList(Iterator<? extends Promise<? extends T>> promises) {
		return toListImpl(promises, 10, false);
	}

	/**
	 * @see Promises#toList(List)
	 */
	@Contract(pure = true)
	private static <T> Promise<List<T>> toListImpl(Iterator<? extends Promise<? extends T>> promises, int initialSize, boolean ownership) {
		PromisesToList<T> resultPromise = new PromisesToList<>(initialSize);

		for (int i = 0; promises.hasNext() && !resultPromise.isComplete(); i++) {
			resultPromise.addToList(i, promises.next());
		}

		if (promises.hasNext()) {
			if (ownership) {
				promises.forEachRemaining(Recyclers::recycle);
			} else {
				Recyclers.recycle(promises);
			}
		}

		return resultPromise.countdown == 0 ? Promise.of(resultPromise.getList()) : resultPromise;
	}

	/**
	 * Returns an array of provided {@code type} and length 0
	 * wrapped in {@code Promise}.
	 */
	@Contract(pure = true)
	public static <T> Promise<T[]> toArray(Class<T> type) {
		return Promise.of((T[]) Array.newInstance(type, 0));
	}

	/**
	 * Returns an array with {@code promise1} result.
	 */
	@Contract(pure = true)
	public static <T> Promise<T[]> toArray(Class<T> type, Promise<? extends T> promise1) {
		return promise1.map(value -> {
			T[] array = (T[]) Array.newInstance(type, 1);
			array[0] = value;
			return array;
		});
	}

	/**
	 * Returns an array with {@code promise1} and {@code promise2} results.
	 */
	@Contract(pure = true)
	public static <T> Promise<T[]> toArray(Class<T> type, Promise<? extends T> promise1, Promise<? extends T> promise2) {
		return promise1.combine(promise2, (value1, value2) -> {
			T[] array = (T[]) Array.newInstance(type, 2);
			array[0] = value1;
			array[1] = value2;
			return array;
		});
	}

	/**
	 * @see Promises#toArray(Class, List)
	 */
	@Contract(pure = true)
	@SafeVarargs
	public static <T> Promise<T[]> toArray(Class<T> type, Promise<? extends T>... promises) {
		return toList(promises).map(list -> list.toArray((T[]) Array.newInstance(type, list.size())));
	}

	/**
	 * Reduces promises into Promise&lt;Array&gt;
	 */
	@Contract(pure = true)
	public static <T> Promise<T[]> toArray(Class<T> type, List<? extends Promise<? extends T>> promises) {
		int size = promises.size();
		if (size == 0) return toArray(type);
		if (size == 1) return toArray(type, promises.get(0));
		if (size == 2) return toArray(type, promises.get(0), promises.get(1));
		return toList(promises).map(list -> list.toArray((T[]) Array.newInstance(type, list.size())));
	}

	/**
	 * @see Promises#toArray(Class, List)
	 */
	@Contract(pure = true)
	public static <T> Promise<T[]> toArray(Class<T> type, Stream<? extends Promise<? extends T>> promises) {
		return toList(promises).map(list -> list.toArray((T[]) Array.newInstance(type, list.size())));
	}

	/**
	 * @see Promises#toArray(Class, List)
	 */
	@Contract(pure = true)
	public static <T> Promise<T[]> toArray(Class<T> type, Iterable<? extends Promise<? extends T>> promises) {
		return toList(promises).map(list -> list.toArray((T[]) Array.newInstance(type, list.size())));
	}

	/**
	 * @see Promises#toArray(Class, List)
	 */
	@Contract(pure = true)
	public static <T> Promise<T[]> toArray(Class<T> type, Iterator<? extends Promise<? extends T>> promises) {
		return toList(promises).map(list -> list.toArray((T[]) Array.newInstance(type, list.size())));
	}

	@Contract(pure = true)
	public static <T1, R> Promise<R> toTuple(TupleConstructor1<T1, R> constructor, Promise<? extends T1> promise1) {
		return promise1.map(constructor::create);
	}

	@Contract(pure = true)
	public static <T1, T2, R> Promise<R> toTuple(TupleConstructor2<T1, T2, R> constructor,
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2) {
		return promise1.combine(promise2, constructor::create);
	}

	@Contract(pure = true)
	public static <T1, T2, T3, R> Promise<R> toTuple(TupleConstructor3<T1, T2, T3, R> constructor,
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2,
			Promise<? extends T3> promise3) {
		return toList(promise1, promise2, promise3)
				.map(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2)));
	}

	@Contract(pure = true)
	public static <T1, T2, T3, T4, R> Promise<R> toTuple(TupleConstructor4<T1, T2, T3, T4, R> constructor,
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2,
			Promise<? extends T3> promise3,
			Promise<? extends T4> promise4) {
		return toList(promise1, promise2, promise3, promise4)
				.map(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3)));
	}

	@Contract(pure = true)
	public static <T1, T2, T3, T4, T5, R> Promise<R> toTuple(TupleConstructor5<T1, T2, T3, T4, T5, R> constructor,
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2,
			Promise<? extends T3> promise3,
			Promise<? extends T4> promise4,
			Promise<? extends T5> promise5) {
		return toList(promise1, promise2, promise3, promise4, promise5)
				.map(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4)));
	}

	@Contract(pure = true)
	public static <T1, T2, T3, T4, T5, T6, R> Promise<R> toTuple(TupleConstructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2,
			Promise<? extends T3> promise3,
			Promise<? extends T4> promise4,
			Promise<? extends T5> promise5,
			Promise<? extends T6> promise6) {
		return toList(promise1, promise2, promise3, promise4, promise5, promise6)
				.map(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4),
						(T6) list.get(5)));
	}

	@Contract(pure = true)
	public static <T1> Promise<Tuple1<T1>> toTuple(Promise<? extends T1> promise1) {
		return promise1.map(Tuple1::new);
	}

	@Contract(pure = true)
	public static <T1, T2> Promise<Tuple2<T1, T2>> toTuple(Promise<? extends T1> promise1, Promise<? extends T2> promise2) {
		return promise1.combine(promise2, Tuple2::new);
	}

	@Contract(pure = true)
	public static <T1, T2, T3> Promise<Tuple3<T1, T2, T3>> toTuple(
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2,
			Promise<? extends T3> promise3) {
		return toList(promise1, promise2, promise3)
				.map(list -> new Tuple3<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2)));
	}

	@Contract(pure = true)
	public static <T1, T2, T3, T4> Promise<Tuple4<T1, T2, T3, T4>> toTuple(
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2,
			Promise<? extends T3> promise3,
			Promise<? extends T4> promise4) {
		return toList(promise1, promise2, promise3, promise4)
				.map(list -> new Tuple4<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3)));
	}

	@Contract(pure = true)
	public static <T1, T2, T3, T4, T5> Promise<Tuple5<T1, T2, T3, T4, T5>> toTuple(
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2,
			Promise<? extends T3> promise3,
			Promise<? extends T4> promise4,
			Promise<? extends T5> promise5) {
		return toList(promise1, promise2, promise3, promise4, promise5)
				.map(list -> new Tuple5<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4)));
	}

	@Contract(pure = true)
	public static <T1, T2, T3, T4, T5, T6> Promise<Tuple6<T1, T2, T3, T4, T5, T6>> toTuple(
			Promise<? extends T1> promise1,
			Promise<? extends T2> promise2,
			Promise<? extends T3> promise3,
			Promise<? extends T4> promise4,
			Promise<? extends T5> promise5,
			Promise<? extends T6> promise6) {
		return toList(promise1, promise2, promise3, promise4, promise5, promise6)
				.map(list -> new Tuple6<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4), (T6) list.get(5)));
	}

	@Contract(pure = true)
	public static <T, T1, R, R1> AsyncFunction<T, R> mapTuple(TupleConstructor1<R1, R> constructor,
			Function<? super T, T1> getter1, Function<T1, Promise<R1>> fn1) {
		return t -> toTuple(constructor,
				fn1.apply(getter1.apply(t)));
	}

	@Contract(pure = true)
	public static <T, T1, T2, R, R1, R2> AsyncFunction<T, R> mapTuple(TupleConstructor2<R1, R2, R> constructor,
			Function<? super T, T1> getter1, Function<T1, Promise<R1>> fn1,
			Function<? super T, T2> getter2, Function<T2, Promise<R2>> fn2) {
		return t -> toTuple(constructor,
				fn1.apply(getter1.apply(t)),
				fn2.apply(getter2.apply(t)));
	}

	@Contract(pure = true)
	public static <T, T1, T2, T3, R, R1, R2, R3> AsyncFunction<T, R> mapTuple(TupleConstructor3<R1, R2, R3, R> constructor,
			Function<? super T, T1> getter1, Function<T1, Promise<R1>> fn1,
			Function<? super T, T2> getter2, Function<T2, Promise<R2>> fn2,
			Function<? super T, T3> getter3, Function<T3, Promise<R3>> fn3) {
		return t -> toTuple(constructor,
				fn1.apply(getter1.apply(t)),
				fn2.apply(getter2.apply(t)),
				fn3.apply(getter3.apply(t)));
	}

	@Contract(pure = true)
	public static <T, T1, T2, T3, T4, R, R1, R2, R3, R4> AsyncFunction<T, R> mapTuple(TupleConstructor4<R1, R2, R3, R4, R> constructor,
			Function<? super T, T1> getter1, Function<T1, Promise<R1>> fn1,
			Function<? super T, T2> getter2, Function<T2, Promise<R2>> fn2,
			Function<? super T, T3> getter3, Function<T3, Promise<R3>> fn3,
			Function<? super T, T4> getter4, Function<T4, Promise<R4>> fn4) {
		return t -> toTuple(constructor,
				fn1.apply(getter1.apply(t)),
				fn2.apply(getter2.apply(t)),
				fn3.apply(getter3.apply(t)),
				fn4.apply(getter4.apply(t)));
	}

	@Contract(pure = true)
	public static <T, T1, T2, T3, T4, T5, R, R1, R2, R3, R4, R5> AsyncFunction<T, R> mapTuple(TupleConstructor5<R1, R2, R3, R4, R5, R> constructor,
			Function<? super T, T1> getter1, Function<T1, Promise<R1>> fn1,
			Function<? super T, T2> getter2, Function<T2, Promise<R2>> fn2,
			Function<? super T, T3> getter3, Function<T3, Promise<R3>> fn3,
			Function<? super T, T4> getter4, Function<T4, Promise<R4>> fn4,
			Function<? super T, T5> getter5, Function<T5, Promise<R5>> fn5) {
		return t -> toTuple(constructor,
				fn1.apply(getter1.apply(t)),
				fn2.apply(getter2.apply(t)),
				fn3.apply(getter3.apply(t)),
				fn4.apply(getter4.apply(t)),
				fn5.apply(getter5.apply(t)));
	}

	@Contract(pure = true)
	public static <T, T1, T2, T3, T4, T5, T6, R, R1, R2, R3, R4, R5, R6> AsyncFunction<T, R> mapTuple(TupleConstructor6<R1, R2, R3, R4, R5, R6, R> constructor,
			Function<? super T, T1> getter1, Function<T1, Promise<R1>> fn1,
			Function<? super T, T2> getter2, Function<T2, Promise<R2>> fn2,
			Function<? super T, T3> getter3, Function<T3, Promise<R3>> fn3,
			Function<? super T, T4> getter4, Function<T4, Promise<R4>> fn4,
			Function<? super T, T5> getter5, Function<T5, Promise<R5>> fn5,
			Function<? super T, T6> getter6, Function<T6, Promise<R6>> fn6) {
		return t -> toTuple(constructor,
				fn1.apply(getter1.apply(t)),
				fn2.apply(getter2.apply(t)),
				fn3.apply(getter3.apply(t)),
				fn4.apply(getter4.apply(t)),
				fn5.apply(getter5.apply(t)),
				fn6.apply(getter6.apply(t)));
	}

	/**
	 * Returns a {@link CompleteNullPromise}
	 */
	public static Promise<Void> sequence() {
		return Promise.complete();
	}

	/**
	 * Executes an {@link AsyncRunnable}, returning a {@link Promise<Void>}
	 * as a mark for completion
	 */
	public static Promise<Void> sequence(AsyncRunnable runnable) {
		return runnable.run();
	}

	/**
	 * Executes both {@link AsyncRunnable}s consequently, returning a {@link Promise<Void>}
	 * as a mark for completion
	 */
	public static Promise<Void> sequence(AsyncRunnable runnable1, AsyncRunnable runnable2) {
		return runnable1.run().then(runnable2::run);
	}

	/**
	 * @see Promises#sequence(Iterator)
	 */
	public static Promise<Void> sequence(AsyncRunnable... runnables) {
		return sequence(List.of(runnables));
	}

	/**
	 * @see Promises#sequence(Iterator)
	 */
	public static Promise<Void> sequence(Iterable<? extends AsyncRunnable> runnables) {
		return sequence(transformIterator(runnables.iterator(), AsyncRunnable::run));
	}

	/**
	 * @see Promises#sequence(Iterator)
	 */
	public static Promise<Void> sequence(Stream<? extends AsyncRunnable> runnables) {
		return sequence(transformIterator(runnables.iterator(), AsyncRunnable::run));
	}

	/**
	 * Calls every {@code Promise} from {@code promises} in sequence and discards
	 * their results.Returns a {@code SettablePromise} with {@code null} result as
	 * a marker when all of the {@code promises} are completed.
	 *
	 * @return {@code Promise} that completes when all {@code promises} are completed
	 */
	public static Promise<Void> sequence(Iterator<? extends Promise<Void>> promises) {
		return Promise.ofCallback(cb ->
				sequenceImpl(promises, cb));
	}

	private static void sequenceImpl(Iterator<? extends Promise<Void>> promises, SettablePromise<Void> cb) {
		while (promises.hasNext()) {
			Promise<?> promise = promises.next();
			if (promise.isResult()) continue;
			promise.run((result, e) -> {
				if (e == null) {
					sequenceImpl(promises, cb);
				} else {
					cb.setException(e);
				}
			});
			return;
		}
		cb.set(null);
	}

	/**
	 * Picks the first {@code Promise} that was completed without exception.
	 *
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@SafeVarargs
	public static <T> Promise<T> first(AsyncSupplier<? extends T>... promises) {
		return first(isResult(), promises);
	}

	/**
	 * @see #first(AsyncSupplier[])
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	public static <T> Promise<T> first(Iterable<? extends AsyncSupplier<? extends T>> promises) {
		return first(isResult(), promises);
	}

	/**
	 * @see #first(AsyncSupplier[])
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	public static <T> Promise<T> first(Stream<? extends AsyncSupplier<? extends T>> promises) {
		return first(isResult(), promises);
	}

	/**
	 * @see #first(AsyncSupplier[])
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	public static <T> Promise<T> first(Iterator<? extends Promise<? extends T>> promises) {
		return first(isResult(), promises);
	}

	/**
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@SafeVarargs
	public static <T> Promise<T> first(BiPredicate<? super T, ? super Exception> predicate,
			AsyncSupplier<? extends T>... promises) {
		return first(predicate, List.of(promises));
	}

	/**
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	public static <T> Promise<T> first(BiPredicate<? super T, ? super Exception> predicate,
			Iterable<? extends AsyncSupplier<? extends T>> promises) {
		return first(predicate, asPromises(promises));
	}

	/**
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	public static <T> Promise<T> first(BiPredicate<? super T, ? super Exception> predicate,
			Stream<? extends AsyncSupplier<? extends T>> promises) {
		return first(predicate, asPromises(promises));
	}

	/**
	 * @param predicate filters results, consumes result of {@code Promise}
	 * @return first completed result of {@code Promise} that satisfies predicate
	 */
	public static <T> Promise<T> first(BiPredicate<? super T, ? super Exception> predicate,
			Iterator<? extends Promise<? extends T>> promises) {
		return Promise.ofCallback(cb ->
				firstImpl(promises, predicate, cb));
	}

	private static <T> void firstImpl(Iterator<? extends Promise<? extends T>> promises,
			BiPredicate<? super T, ? super Exception> predicate,
			SettablePromise<T> cb) {
		while (promises.hasNext()) {
			Promise<? extends T> nextPromise = promises.next();
			if (nextPromise.isComplete()) {
				T v = nextPromise.getResult();
				Exception e = nextPromise.getException();
				if (predicate.test(v, e)) {
					cb.accept(v, e);
					return;
				}
				Recyclers.recycle(v);
				continue;
			}
			nextPromise.run((v, e) -> {
				if (predicate.test(v, e)) {
					cb.accept(v, e);
				} else {
					Recyclers.recycle(v);
					firstImpl(promises, predicate, cb);
				}
			});
			return;
		}
		cb.setException(new Exception("There are no promises to be complete"));
	}

	/**
	 * Repeats the operations of provided {@link AsyncSupplier<Boolean>} infinitely,
	 * until one of the {@code Promise}s completes exceptionally or supplier returns a promise of {@code false}.
	 */
	public static Promise<Void> repeat(AsyncSupplier<Boolean> supplier) {
		SettablePromise<Void> cb = new SettablePromise<>();
		repeatImpl(supplier, cb);
		return cb;
	}

	private static void repeatImpl(AsyncSupplier<Boolean> supplier, SettablePromise<Void> cb) {
		while (true) {
			Promise<Boolean> promise = supplier.get();
			if (promise.isResult()) {
				if (promise.getResult() == Boolean.TRUE) continue;
				cb.set(null);
				break;
			}
			promise.run((b, e) -> {
				if (e == null) {
					if (b == Boolean.TRUE) {
						repeatImpl(supplier, cb);
					} else {
						cb.set(null);
					}
				} else {
					cb.setException(e);
				}
			});
			break;
		}
	}

	/**
	 * Repeats provided {@link FunctionEx} until can pass {@link Predicate} test.
	 * Resembles a simple Java {@code for()} loop but with async capabilities.
	 *
	 * @param seed          start value
	 * @param loopCondition a boolean function which checks if this loop can continue
	 * @param next          a function applied to the seed, returns {@code Promise}
	 * @return {@link SettablePromise} with {@code null} result if it was
	 * completed successfully, otherwise returns a {@code SettablePromise}
	 * with an exception. In both situations returned {@code Promise}
	 * is a marker of completion of the loop.
	 */
	public static <T> Promise<T> loop(@Nullable T seed, Predicate<T> loopCondition, FunctionEx<T, Promise<T>> next) {
		if (!loopCondition.test(seed)) return Promise.of(seed);
		return until(seed, next, v -> !loopCondition.test(v));
	}

	public static <T> Promise<T> until(@Nullable T seed, FunctionEx<T, Promise<T>> next, Predicate<T> breakCondition) {
		return Promise.ofCallback(cb ->
				untilImpl(seed, next, breakCondition, cb));
	}

	private static <T> void untilImpl(@Nullable T value, FunctionEx<T, Promise<T>> next, Predicate<T> breakCondition, SettablePromise<T> cb) throws Exception {
		while (true) {
			Promise<T> promise = next.apply(value);
			if (promise.isResult()) {
				value = promise.getResult();
				if (breakCondition.test(value)) {
					cb.set(value);
					break;
				}
			} else {
				promise.run((newValue, e) -> {
					if (e == null) {
						if (breakCondition.test(newValue)) {
							cb.set(newValue);
						} else {
							try {
								untilImpl(newValue, next, breakCondition, cb);
							} catch (Exception ex) {
								handleError(ex, next);
								cb.setException(ex);
							}
						}
					} else {
						cb.setException(e);
					}
				});
				return;
			}
		}
	}

	public static <T> Promise<T> retry(AsyncSupplier<T> supplier) {
		return retry(isResult(), supplier);
	}

	public static <T> Promise<T> retry(BiPredicate<? super T, Exception> breakCondition, AsyncSupplier<T> supplier) {
		return first(breakCondition, Stream.generate(() -> supplier));
	}

	public static <T> Promise<T> retry(AsyncSupplier<T> supplier, RetryPolicy<?> retryPolicy) {
		return retry(supplier, (v, e) -> e == null, retryPolicy);
	}

	public static <T> Promise<T> retry(AsyncSupplier<T> supplier, BiPredicate<T, Exception> breakCondition, RetryPolicy<?> retryPolicy) {
		return Promise.ofCallback(cb ->
				retryImpl(supplier, breakCondition, (RetryPolicy<Object>) retryPolicy, null, cb));
	}

	private static <T> void retryImpl(AsyncSupplier<? extends T> next, BiPredicate<T, Exception> breakCondition,
			RetryPolicy<Object> retryPolicy, Object retryState,
			SettablePromise<T> cb) {
		next.get()
				.run((v, e) -> {
					if (breakCondition.test(v, e)) {
						cb.accept(v, e);
					} else {
						Reactor reactor = getCurrentReactor();
						long now = reactor.currentTimeMillis();
						Object retryStateFinal = retryState != null ? retryState : retryPolicy.createRetryState();
						long nextRetryTimestamp = retryPolicy.nextRetryTimestamp(now, e, retryStateFinal);
						if (nextRetryTimestamp == 0) {
							cb.setException(e != null ? e : new Exception("RetryPolicy: giving up " + retryState));
						} else {
							reactor.schedule(nextRetryTimestamp,
									wrapContext(cb, () -> retryImpl(next, breakCondition, retryPolicy, retryStateFinal, cb)));
						}
					}
				});
	}

	/**
	 * Transforms a collection of {@link AsyncSupplier}
	 * {@code tasks} to a collection of {@code Promise}s.
	 */
	public static <T> Iterator<Promise<T>> asPromises(Iterator<? extends AsyncSupplier<? extends T>> tasks) {
		return transformIterator((Iterator<AsyncSupplier<T>>) tasks, AsyncSupplier::get);
	}

	/**
	 * Transforms a {@link Stream} of {@link AsyncSupplier}
	 * {@code tasks} to a collection of {@code Promise}s.
	 */
	public static <T> Iterator<Promise<T>> asPromises(Stream<? extends AsyncSupplier<? extends T>> tasks) {
		return asPromises(tasks.iterator());
	}

	/**
	 * Transforms an {@link Iterable} of {@link AsyncSupplier}
	 * {@code tasks} to a collection of {@code Promise}s.
	 */
	public static <T> Iterator<Promise<T>> asPromises(Iterable<? extends AsyncSupplier<? extends T>> tasks) {
		return asPromises(tasks.iterator());
	}

	/**
	 * Transforms an {@link AsyncSupplier} {@code tasks}
	 * to a collection of {@code Promise}s.
	 */
	@SafeVarargs
	public static <T> Iterator<Promise<T>> asPromises(AsyncSupplier<? extends T>... tasks) {
		return asPromises(iteratorOf(tasks));
	}

	/**
	 * @see #reduce(A, BiConsumerEx, FunctionEx, Iterator)
	 */
	public static <T, A, R> Promise<R> reduce(@Nullable A accumulator, BiConsumerEx<A, T> combiner, FunctionEx<A, R> finisher, Collection<Promise<T>> promises) {
		return reduce(accumulator, combiner, finisher, promises.iterator());
	}

	/**
	 * @see #reduce(A, BiConsumerEx, FunctionEx, Iterator)
	 */
	public static <T, A, R> Promise<R> reduce(@Nullable A accumulator, BiConsumerEx<A, T> combiner, FunctionEx<A, R> finisher, Stream<Promise<T>> promises) {
		return reduce(accumulator, combiner, finisher, promises.iterator());
	}

	/**
	 * Asynchronously reduce {@link Iterator} of {@link Promise<T>}s into a {@link Promise<R>}.
	 * <p>
	 * To reduce promises a following arguments should be supplied an <b>accumulator</b>, a <b>combiner</b>,
	 * a <b>finisher</b> and an iterator of <b>promises</b>
	 * <p>
	 * Reduction principle is somewhat similar to the {@link Collector#of(Supplier, BiConsumer, BinaryOperator, Function, Collector.Characteristics...)}
	 * <p>
	 * If one of the {@link Promise}s completes exceptionally, a resulting promise will be completed exceptionally as well.
	 *
	 * @param accumulator a promise result accumulator that holds intermediate promise results
	 * @param combiner    a combiner consumer that defines how promise results should be accumulated
	 * @param finisher    a finisher function that maps an accumulator to a result value
	 * @param promises    {@code Iterator} of {@code Promise}s
	 * @param <T>         type of input elements for this operation
	 * @param <A>         type of accumulator of intermediate promise results
	 * @param <R>         the result type of the reduction
	 * @return a {@code Promise} of the accumulated result of the reduction.
	 */
	public static <T, A, R> Promise<R> reduce(@Nullable A accumulator, BiConsumerEx<A, T> combiner, FunctionEx<A, R> finisher, Iterator<Promise<T>> promises) {
		AsyncAccumulator<A> asyncAccumulator = AsyncAccumulator.create(accumulator);
		while (promises.hasNext()) {
			asyncAccumulator.addPromise(promises.next(), combiner);
		}
		return asyncAccumulator.run().map(finisher);
	}

	/**
	 * Asynchronously reduce {@link Iterator} of {@link Promise<T>}s into a {@link Promise<R>}
	 * with the help of {@link Collector}.
	 * <p>
	 * You can control the amount of concurrently running {@code Promise}s.
	 * <p>
	 * If one of the {@link Promise}s completes exceptionally, a resulting promise will be completed exceptionally as well.
	 * <p>
	 * This method is universal and allows implementing app-specific logic.
	 *
	 * @param collector mutable reduction operation that accumulates input
	 *                  elements into a mutable result container
	 * @param maxCalls  max number of concurrently running {@code Promise}s
	 * @param promises  {@code Iterable} of {@code Promise}s
	 * @param <T>       type of input elements for this operation
	 * @param <A>       type of accumulator of intermediate promise results
	 * @param <R>       the result type of the reduction
	 * @return a {@code Promise} of the accumulated result of the reduction.
	 */
	public static <T, A, R> Promise<R> reduce(Collector<T, A, R> collector, int maxCalls,
			Iterator<Promise<T>> promises) {
		return reduce(collector.supplier().get(), BiConsumerEx.of(collector.accumulator()), FunctionEx.of(collector.finisher()),
				maxCalls, promises);
	}

	/**
	 * Asynchronously reduce {@link Iterator} of {@link Promise<T>}s into a {@link Promise<R>}.
	 * <p>
	 * To reduce promises a following arguments should be supplied an <b>accumulator</b>, a <b>combiner</b>,
	 * a <b>finisher</b> and an iterator of <b>promises</b>
	 * <p>
	 * You can control the amount of concurrently running {@code Promise}s.
	 * <p>
	 * Reduction principle is somewhat similar to the {@link Collector#of(Supplier, BiConsumer, BinaryOperator, Function, Collector.Characteristics...)}
	 * <p>
	 * If one of the {@link Promise}s completes exceptionally, a resulting promise will be completed exceptionally as well.
	 *
	 * @param accumulator a promise result accumulator that holds intermediate promise results
	 * @param combiner    a combiner consumer that defines how promise results should be accumulated
	 * @param finisher    a finisher function that maps an accumulator to a result value
	 * @param maxCalls    max number of concurrently running {@code Promise}s
	 * @param promises    {@code Iterator} of {@code Promise}s
	 * @param <T>         type of input elements for this operation
	 * @param <A>         type of accumulator of intermediate promise results
	 * @param <R>         the result type of the reduction
	 * @return a {@code Promise} of the accumulated result of the reduction.
	 */
	public static <T, A, R> Promise<R> reduce(@Nullable A accumulator, BiConsumerEx<A, T> combiner, FunctionEx<A, R> finisher, int maxCalls, Iterator<Promise<T>> promises) {
		AsyncAccumulator<A> asyncAccumulator = AsyncAccumulator.create(accumulator);
		for (int i = 0; promises.hasNext() && i < maxCalls; i++) {
			reduceImpl(asyncAccumulator, combiner, promises);
		}
		return asyncAccumulator.run().map(finisher);
	}

	private static <T, A> void reduceImpl(AsyncAccumulator<A> accumulator, BiConsumerEx<A, T> combiner, Iterator<Promise<T>> promises) {
		while (promises.hasNext()) {
			Promise<T> promise = promises.next();
			if (promise.isComplete()) {
				accumulator.addPromise(promise, combiner);
			} else {
				accumulator.addPromise(
						promise.whenResult(() -> reduceImpl(accumulator, combiner, promises)),
						combiner);
				break;
			}
		}
	}

	@Contract(pure = true)
	public static <T, A, R> AsyncFunction<T, R> coalesce(Supplier<A> argumentAccumulatorSupplier, BiConsumer<A, T> argumentAccumulatorFn,
			AsyncFunction<A, R> fn) {
		AsyncBuffer<A, R> buffer = new AsyncBuffer<>(fn, argumentAccumulatorSupplier);
		return v -> {
			Promise<R> promise = buffer.add(argumentAccumulatorFn, v);
			if (!buffer.isActive()) {
				repeat(() -> buffer.flush().map($ -> buffer.isBuffered()));
			}
			return promise;
		};
	}

	// region helper classes
	public static final class PromiseAll<T> extends NextPromise<T, Void> {
		int countdown = 1;

		@Override
		public void accept(@Nullable T result, @Nullable Exception e) {
			if (e == null) {
				Recyclers.recycle(result);
				if (--countdown == 0) {
					complete(null);
				}
			} else {
				tryCompleteExceptionally(e);
			}
		}

		@Override
		protected String describe() {
			return "Promises.all()";
		}
	}

	public static final class PromiseAny<T> extends NextPromise<T, T> {
		private final BiPredicate<? super T, ? super Exception> predicate;
		int countdown = 1;

		private PromiseAny(BiPredicate<? super T, ? super Exception> predicate) {
			this.predicate = predicate;
		}

		@Override
		public void accept(@Nullable T result, @Nullable Exception e) {
			if (predicate.test(result, e)) {
				if (!tryComplete(result, e)) {
					Recyclers.recycle(result);
				}
			} else {
				Recyclers.recycle(result);
				if (--countdown == 0) {
					completeExceptionally(new Exception("There are no promises to be complete"));
				}
			}
		}

		@Override
		protected String describe() {
			return "Promises.any()";
		}
	}

	public static final class PromisesToList<T> extends AbstractPromise<List<T>> {
		Object[] array;
		int countdown;
		int size;

		private PromisesToList(int initialSize) {
			this.array = new Object[initialSize];
		}

		private void addToList(int i, Promise<? extends T> promise) {
			ensureSize(i + 1);
			if (promise.isResult()) {
				if (!isComplete()) {
					array[i] = promise.getResult();
				} else {
					Recyclers.recycle(result);
				}
			} else if (promise.isException()) {
				if (tryCompleteExceptionally(promise.getException())) {
					Recyclers.recycle(array);
				}
			} else {
				countdown++;
				promise.run((result, e) -> {
					if (e == null) {
						if (!isComplete()) {
							array[i] = result;
							if (--countdown == 0) {
								complete(getList());
							}
						} else {
							Recyclers.recycle(result);
						}
					} else {
						if (tryCompleteExceptionally(e)) {
							Recyclers.recycle(array);
						}
					}
				});
			}
		}

		private void ensureSize(int size) {
			this.size = size;
			if (size >= array.length) {
				array = Arrays.copyOf(array, array.length * 2);
			}
		}

		private List<T> getList() {
			return (List<T>) asList(size == this.array.length ? this.array : Arrays.copyOf(this.array, size));
		}

		@Override
		protected String describe() {
			return "Promises.toList()";
		}
	}

	// endregion

}
