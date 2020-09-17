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
import io.activej.async.function.AsyncSupplier;
import io.activej.common.exception.AsyncTimeoutException;
import io.activej.common.exception.StacklessException;
import io.activej.common.recycle.Recyclers;
import io.activej.common.tuple.*;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.schedule.ScheduledRunnable;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Array;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static io.activej.common.Utils.nullify;
import static io.activej.common.collection.CollectionUtils.asIterator;
import static io.activej.common.collection.CollectionUtils.transformIterator;
import static io.activej.eventloop.Eventloop.getCurrentEventloop;
import static io.activej.eventloop.util.RunnableWithContext.wrapContext;
import static io.activej.promise.Promise.NOT_ENOUGH_PROMISES_EXCEPTION;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Allows to manage multiple {@link Promise}s.
 */
@SuppressWarnings({"WeakerAccess", "unchecked"})
public final class Promises {
	public static final AsyncTimeoutException TIMEOUT_EXCEPTION = new AsyncTimeoutException(Promises.class, "Promise timeout");

	/**
	 * @see #timeout(long, Promise)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> timeout(@NotNull Duration delay, @NotNull Promise<T> promise) {
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
	@NotNull
	public static <T> Promise<T> timeout(long delay, @NotNull Promise<T> promise) {
		if (promise.isComplete()) return promise;
		if (delay <= 0) return Promise.ofException(TIMEOUT_EXCEPTION);
		return promise.next(new NextPromise<T, T>() {
			@Nullable
			ScheduledRunnable schedule = getCurrentEventloop().delay(delay,
					wrapContext(this, () -> {
						promise.whenResult(Recyclers::recycle);
						schedule = null;
						tryCompleteExceptionally(TIMEOUT_EXCEPTION);
					}));

			@Override
			public void accept(T result, @Nullable Throwable e) {
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
	@NotNull
	public static Promise<Void> delay(@NotNull Duration delay) {
		return delay(delay.toMillis(), (Void) null);
	}

	@Contract(pure = true)
	@NotNull
	public static Promise<Void> delay(long delayMillis) {
		return delay(delayMillis, (Void) null);
	}

	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> delay(@NotNull Duration delay, T value) {
		return delay(delay.toMillis(), value);
	}

	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> delay(long delayMillis, T value) {
		if (delayMillis <= 0) return Promise.of(value);
		SettablePromise<T> cb = new SettablePromise<>();
		getCurrentEventloop().delay(delayMillis, wrapContext(cb, () -> cb.set(value)));
		return cb;
	}

	/**
	 * @see #delay(long, Promise)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> delay(@NotNull Duration delay, @NotNull Promise<T> promise) {
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
	@NotNull
	public static <T> Promise<T> delay(long delayMillis, @NotNull Promise<T> promise) {
		if (delayMillis <= 0) return promise;
		return Promise.ofCallback(cb ->
				getCurrentEventloop().delay(delayMillis, wrapContext(promise, () -> promise.whenComplete(cb))));
	}

	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> interval(@NotNull Duration interval, @NotNull Promise<T> promise) {
		return interval(interval.toMillis(), promise);
	}

	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> interval(long intervalMillis, @NotNull Promise<T> promise) {
		return intervalMillis <= 0 ?
				promise :
				promise.then(value -> Promise.ofCallback(cb -> getCurrentEventloop().delay(intervalMillis, wrapContext(promise, () -> cb.set(value)))));
	}

	/**
	 * @see #schedule(Promise, long)
	 */
	@Contract(pure = true)
	@NotNull
	public static Promise<Void> schedule(@NotNull Instant instant) {
		return schedule((Void) null, instant.toEpochMilli());
	}

	/**
	 * @see #schedule(Promise, long)
	 */
	@Contract(pure = true)
	@NotNull
	public static Promise<Void> schedule(long timestamp) {
		return schedule((Void) null, timestamp);
	}

	/**
	 * @see #schedule(Promise, long)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> schedule(T value, @NotNull Instant instant) {
		return schedule(value, instant.toEpochMilli());
	}

	/**
	 * @see #schedule(Promise, long)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> schedule(T value, long timestamp) {
		SettablePromise<T> cb = new SettablePromise<>();
		getCurrentEventloop().schedule(timestamp, wrapContext(cb, () -> cb.set(value)));
		return cb;
	}

	/**
	 * @see #schedule(Promise, long)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> schedule(@NotNull Promise<T> promise, @NotNull Instant instant) {
		return schedule(promise, instant.toEpochMilli());
	}

	/**
	 * Schedules completion of the {@code Promise} so that it will
	 * be completed after the timestamp even if its operations
	 * were completed earlier.
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> schedule(@NotNull Promise<T> promise, long timestamp) {
		return Promise.ofCallback(cb ->
				getCurrentEventloop().schedule(timestamp, wrapContext(promise, () -> promise.whenComplete(cb))));
	}

	/**
	 * @see Promises#all(List)
	 */
	@Contract(pure = true)
	@NotNull
	public static Promise<Void> all() {
		return Promise.complete();
	}

	/**
	 * @see Promises#all(List)
	 */
	@Contract(pure = true)
	@NotNull
	public static Promise<Void> all(@NotNull Promise<?> promise1) {
		return promise1.toVoid();
	}

	/**
	 * Optimized for 2 promises.
	 *
	 * @see Promises#all(List)
	 */
	@Contract(pure = true)
	@NotNull
	public static Promise<Void> all(@NotNull Promise<?> promise1, @NotNull Promise<?> promise2) {
		return promise1.both(promise2);
	}

	/**
	 * @see Promises#all(List)
	 */
	@Contract(pure = true)
	@NotNull
	public static Promise<Void> all(@NotNull Promise<?>... promises) {
		return all(asList(promises));
	}

	/**
	 * Returns a {@code Promise} that completes when
	 * all of the {@code promises} are completed.
	 */
	@Contract(pure = true)
	@NotNull
	public static Promise<Void> all(@NotNull List<? extends Promise<?>> promises) {
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
	@NotNull
	public static Promise<Void> all(@NotNull Stream<? extends Promise<?>> promises) {
		return all(promises.iterator());
	}

	/**
	 * Returns {@code Promise} that completes when all of the {@code promises}
	 * are completed. If at least one of the {@code promises} completes
	 * exceptionally, a {@link CompleteExceptionallyPromise} will be returned.
	 */
	@NotNull
	public static Promise<Void> all(@NotNull Iterator<? extends Promise<?>> promises) {
		return allIterator(promises, false);
	}

	@NotNull
	private static Promise<Void> allIterator(@NotNull Iterator<? extends Promise<?>> promises, boolean ownership) {
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
			promise.whenComplete(resultPromise);
		}
		resultPromise.countdown--;
		return resultPromise.countdown == 0 ? Promise.complete() : resultPromise;
	}

	/**
	 * Returns a {@link CompleteExceptionallyPromise} with {@link StacklessException},
	 * since this method doesn't accept any {@code Promise}s
	 *
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> any() {
		return Promise.ofException(NOT_ENOUGH_PROMISES_EXCEPTION);
	}

	/**
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> any(@NotNull Promise<? extends T> promise1) {
		return (Promise<T>) promise1;
	}

	/**
	 * Optimized for 2 promises.
	 *
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> any(@NotNull Promise<? extends T> promise1, @NotNull Promise<? extends T> promise2) {
		return ((Promise<T>) promise1).either(promise2);
	}

	/**
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	@NotNull
	@SafeVarargs
	public static <T> Promise<T> any(@NotNull Promise<? extends T>... promises) {
		return any(isResult(), asList(promises));
	}

	/**
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> any(@NotNull List<? extends Promise<? extends T>> promises) {
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
	@NotNull
	public static <T> Promise<T> any(@NotNull Stream<? extends Promise<? extends T>> promises) {
		return any(isResult(), promises.iterator());
	}

	@NotNull
	public static <T> Promise<T> any(@NotNull Iterator<? extends Promise<? extends T>> promises) {
		return any(isResult(), promises);
	}

	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> any(@NotNull BiPredicate<T, Throwable> predicate, @NotNull Promise<? extends T> promise1) {
		return any(predicate, singletonList(promise1));
	}

	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> any(@NotNull BiPredicate<T, Throwable> predicate, @NotNull Promise<? extends T> promise1, @NotNull Promise<? extends T> promise2) {
		return any(predicate, asList(promise1, promise2));
	}

	@Contract(pure = true)
	@NotNull
	@SafeVarargs
	public static <T> Promise<T> any(@NotNull BiPredicate<T, Throwable> predicate, @NotNull Promise<? extends T>... promises) {
		return any(predicate, asList(promises));
	}

	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> any(@NotNull BiPredicate<T, Throwable> predicate, @NotNull Stream<? extends Promise<? extends T>> promises) {
		return any(predicate, promises.iterator());
	}

	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> any(@NotNull BiPredicate<T, Throwable> predicate, @NotNull List<? extends Promise<? extends T>> promises) {
		return anyIterator(predicate, promises.iterator(), true);
	}

	@NotNull
	public static <T> Promise<T> any(@NotNull BiPredicate<T, Throwable> predicate, @NotNull Iterator<? extends Promise<? extends T>> promises) {
		return anyIterator(predicate, promises, false);
	}

	@NotNull
	private static <T> Promise<T> anyIterator(@NotNull BiPredicate<T, Throwable> predicate,
			@NotNull Iterator<? extends Promise<? extends T>> promises, boolean ownership) {
		if (!promises.hasNext()) return any();
		PromiseAny<T> resultPromise = new PromiseAny<>(predicate);
		while (promises.hasNext()) {
			Promise<? extends T> promise = promises.next();
			if (promise.isComplete()) {
				T result = promise.getResult();
				if (predicate.test(result, promise.getException())) {
					if (ownership){
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
			promise.whenComplete(resultPromise);
		}
		resultPromise.countdown--;
		return resultPromise.countdown == 0 ? any() : resultPromise;
	}

	/**
	 * Returns a successfully completed {@code Promise}
	 * with an empty list as the result.
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> toList() {
		return Promise.of(emptyList());
	}

	/**
	 * Returns a completed {@code Promise}
	 * with a result wrapped in {@code List}.
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> toList(@NotNull Promise<? extends T> promise1) {
		return promise1.map(Collections::singletonList);
	}

	/**
	 * Returns {@code Promise} with a list of {@code promise1} and {@code promise2} results.
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> toList(@NotNull Promise<? extends T> promise1, @NotNull Promise<? extends T> promise2) {
		return promise1.combine(promise2, Arrays::asList);
	}

	/**
	 * @see Promises#toList(List)
	 */
	@Contract(pure = true)
	@NotNull
	@SafeVarargs
	public static <T> Promise<List<T>> toList(@NotNull Promise<? extends T>... promises) {
		return toList(asList(promises));
	}

	/**
	 * Reduces list of {@code Promise}s into Promise&lt;List&gt;.
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> toList(@NotNull List<? extends Promise<? extends T>> promises) {
		int size = promises.size();
		if (size == 0) return Promise.of(Collections.emptyList());
		if (size == 1) return promises.get(0).map(Collections::singletonList);
		if (size == 2) return promises.get(0).combine(promises.get(1), Arrays::asList);
		return toListImpl(promises.iterator(), promises.size(), true);
	}

	/**
	 * @see Promises#toList(List)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> toList(@NotNull Stream<? extends Promise<? extends T>> promises) {
		return toList(promises.iterator());
	}

	/**
	 * @see Promises#toList(List)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> toList(@NotNull Iterable<? extends Promise<? extends T>> promises) {
		return toList(promises.iterator());
	}

	/**
	 * @see Promises#toList(List)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> toList(@NotNull Iterator<? extends Promise<? extends T>> promises) {
		return toListImpl(promises, 10, false);
	}

	/**
	 * @see Promises#toList(List)
	 */
	@Contract(pure = true)
	@NotNull
	private static <T> Promise<List<T>> toListImpl(@NotNull Iterator<? extends Promise<? extends T>> promises, int initialSize, boolean ownership) {
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
	@NotNull
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type) {
		return Promise.of((T[]) Array.newInstance(type, 0));
	}

	/**
	 * Returns an array with {@code promise1} result.
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type, @NotNull Promise<? extends T> promise1) {
		return promise1.map(value -> {
			@NotNull T[] array = (T[]) Array.newInstance(type, 1);
			array[0] = value;
			return array;
		});
	}

	/**
	 * Returns an array with {@code promise1} and {@code promise2} results.
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type, @NotNull Promise<? extends T> promise1, @NotNull Promise<? extends T> promise2) {
		return promise1.combine(promise2, (value1, value2) -> {
			@NotNull T[] array = (T[]) Array.newInstance(type, 2);
			array[0] = value1;
			array[1] = value2;
			return array;
		});
	}

	/**
	 * @see Promises#toArray(Class, List)
	 */
	@Contract(pure = true)
	@NotNull
	@SafeVarargs
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type, @NotNull Promise<? extends T>... promises) {
		return toList(promises).map(list -> list.toArray((T[]) Array.newInstance(type, list.size())));
	}

	/**
	 * Reduces promises into Promise&lt;Array&gt;
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type, @NotNull List<? extends Promise<? extends T>> promises) {
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
	@NotNull
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type, @NotNull Stream<? extends Promise<? extends T>> promises) {
		return toList(promises).map(list -> list.toArray((T[]) Array.newInstance(type, list.size())));
	}

	/**
	 * @see Promises#toArray(Class, List)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type, @NotNull Iterable<? extends Promise<? extends T>> promises) {
		return toList(promises).map(list -> list.toArray((T[]) Array.newInstance(type, list.size())));
	}

	/**
	 * @see Promises#toArray(Class, List)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type, @NotNull Iterator<? extends Promise<? extends T>> promises) {
		return toList(promises).map(list -> list.toArray((T[]) Array.newInstance(type, list.size())));
	}

	@Contract(pure = true)
	@NotNull
	public static <T1, R> Promise<R> toTuple(@NotNull TupleConstructor1<T1, R> constructor, @NotNull Promise<? extends T1> promise1) {
		return promise1.map(constructor::create);
	}

	@Contract(pure = true)
	@NotNull
	public static <T1, T2, R> Promise<R> toTuple(@NotNull TupleConstructor2<T1, T2, R> constructor,
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2) {
		return promise1.combine(promise2, constructor::create);
	}

	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3, R> Promise<R> toTuple(@NotNull TupleConstructor3<T1, T2, T3, R> constructor,
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3) {
		return toList(promise1, promise2, promise3)
				.map(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2)));
	}

	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3, T4, R> Promise<R> toTuple(@NotNull TupleConstructor4<T1, T2, T3, T4, R> constructor,
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3,
			@NotNull Promise<? extends T4> promise4) {
		return toList(promise1, promise2, promise3, promise4)
				.map(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3)));
	}

	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3, T4, T5, R> Promise<R> toTuple(@NotNull TupleConstructor5<T1, T2, T3, T4, T5, R> constructor,
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3,
			@NotNull Promise<? extends T4> promise4,
			@NotNull Promise<? extends T5> promise5) {
		return toList(promise1, promise2, promise3, promise4, promise5)
				.map(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4)));
	}

	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3, T4, T5, T6, R> Promise<R> toTuple(@NotNull TupleConstructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3,
			@NotNull Promise<? extends T4> promise4,
			@NotNull Promise<? extends T5> promise5,
			@NotNull Promise<? extends T6> promise6) {
		return toList(promise1, promise2, promise3, promise4, promise5, promise6)
				.map(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4),
						(T6) list.get(5)));
	}

	@Contract(pure = true)
	@NotNull
	public static <T1> Promise<Tuple1<T1>> toTuple(@NotNull Promise<? extends T1> promise1) {
		return promise1.map(Tuple1::new);
	}

	@Contract(pure = true)
	@NotNull
	public static <T1, T2> Promise<Tuple2<T1, T2>> toTuple(@NotNull Promise<? extends T1> promise1, @NotNull Promise<? extends T2> promise2) {
		return promise1.combine(promise2, Tuple2::new);
	}

	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3> Promise<Tuple3<T1, T2, T3>> toTuple(
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3) {
		return toList(promise1, promise2, promise3)
				.map(list -> new Tuple3<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2)));
	}

	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3, T4> Promise<Tuple4<T1, T2, T3, T4>> toTuple(
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3,
			@NotNull Promise<? extends T4> promise4) {
		return toList(promise1, promise2, promise3, promise4)
				.map(list -> new Tuple4<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3)));
	}

	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3, T4, T5> Promise<Tuple5<T1, T2, T3, T4, T5>> toTuple(
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3,
			@NotNull Promise<? extends T4> promise4,
			@NotNull Promise<? extends T5> promise5) {
		return toList(promise1, promise2, promise3, promise4, promise5)
				.map(list -> new Tuple5<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4)));
	}

	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3, T4, T5, T6> Promise<Tuple6<T1, T2, T3, T4, T5, T6>> toTuple(
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3,
			@NotNull Promise<? extends T4> promise4,
			@NotNull Promise<? extends T5> promise5,
			@NotNull Promise<? extends T6> promise6) {
		return toList(promise1, promise2, promise3, promise4, promise5, promise6)
				.map(list -> new Tuple6<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4), (T6) list.get(5)));
	}

	@Contract(pure = true)
	@NotNull
	public static <T, T1, R, R1> Function<T, Promise<R>> mapTuple(@NotNull TupleConstructor1<R1, R> constructor,
			@NotNull Function<? super T, T1> getter1, Function<T1, ? extends Promise<R1>> fn1) {
		return t -> toTuple(constructor,
				fn1.apply(getter1.apply(t)));
	}

	@Contract(pure = true)
	@NotNull
	public static <T, T1, T2, R, R1, R2> Function<T, Promise<R>> mapTuple(@NotNull TupleConstructor2<R1, R2, R> constructor,
			@NotNull Function<? super T, T1> getter1, Function<T1, ? extends Promise<R1>> fn1,
			@NotNull Function<? super T, T2> getter2, Function<T2, ? extends Promise<R2>> fn2) {
		return t -> toTuple(constructor,
				fn1.apply(getter1.apply(t)),
				fn2.apply(getter2.apply(t)));
	}

	@Contract(pure = true)
	@NotNull
	public static <T, T1, T2, T3, R, R1, R2, R3> Function<T, Promise<R>> mapTuple(@NotNull TupleConstructor3<R1, R2, R3, R> constructor,
			@NotNull Function<? super T, T1> getter1, Function<T1, ? extends Promise<R1>> fn1,
			@NotNull Function<? super T, T2> getter2, Function<T2, ? extends Promise<R2>> fn2,
			@NotNull Function<? super T, T3> getter3, Function<T3, ? extends Promise<R3>> fn3) {
		return t -> toTuple(constructor,
				fn1.apply(getter1.apply(t)),
				fn2.apply(getter2.apply(t)),
				fn3.apply(getter3.apply(t)));
	}

	@Contract(pure = true)
	@NotNull
	public static <T, T1, T2, T3, T4, R, R1, R2, R3, R4> Function<T, Promise<R>> mapTuple(@NotNull TupleConstructor4<R1, R2, R3, R4, R> constructor,
			@NotNull Function<? super T, T1> getter1, Function<T1, ? extends Promise<R1>> fn1,
			@NotNull Function<? super T, T2> getter2, Function<T2, ? extends Promise<R2>> fn2,
			@NotNull Function<? super T, T3> getter3, Function<T3, ? extends Promise<R3>> fn3,
			@NotNull Function<? super T, T4> getter4, Function<T4, ? extends Promise<R4>> fn4) {
		return t -> toTuple(constructor,
				fn1.apply(getter1.apply(t)),
				fn2.apply(getter2.apply(t)),
				fn3.apply(getter3.apply(t)),
				fn4.apply(getter4.apply(t)));
	}

	@Contract(pure = true)
	@NotNull
	public static <T, T1, T2, T3, T4, T5, R, R1, R2, R3, R4, R5> Function<T, Promise<R>> mapTuple(@NotNull TupleConstructor5<R1, R2, R3, R4, R5, R> constructor,
			@NotNull Function<? super T, T1> getter1, Function<T1, ? extends Promise<R1>> fn1,
			@NotNull Function<? super T, T2> getter2, Function<T2, ? extends Promise<R2>> fn2,
			@NotNull Function<? super T, T3> getter3, Function<T3, ? extends Promise<R3>> fn3,
			@NotNull Function<? super T, T4> getter4, Function<T4, ? extends Promise<R4>> fn4,
			@NotNull Function<? super T, T5> getter5, Function<T5, ? extends Promise<R5>> fn5) {
		return t -> toTuple(constructor,
				fn1.apply(getter1.apply(t)),
				fn2.apply(getter2.apply(t)),
				fn3.apply(getter3.apply(t)),
				fn4.apply(getter4.apply(t)),
				fn5.apply(getter5.apply(t)));
	}

	@Contract(pure = true)
	@NotNull
	public static <T, T1, T2, T3, T4, T5, T6, R, R1, R2, R3, R4, R5, R6> Function<T, Promise<R>> mapTuple(@NotNull TupleConstructor6<R1, R2, R3, R4, R5, R6, R> constructor,
			@NotNull Function<? super T, T1> getter1, Function<T1, ? extends Promise<R1>> fn1,
			@NotNull Function<? super T, T2> getter2, Function<T2, ? extends Promise<R2>> fn2,
			@NotNull Function<? super T, T3> getter3, Function<T3, ? extends Promise<R3>> fn3,
			@NotNull Function<? super T, T4> getter4, Function<T4, ? extends Promise<R4>> fn4,
			@NotNull Function<? super T, T5> getter5, Function<T5, ? extends Promise<R5>> fn5,
			@NotNull Function<? super T, T6> getter6, Function<T6, ? extends Promise<R6>> fn6) {
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
	@NotNull
	public static Promise<Void> sequence() {
		return Promise.complete();
	}

	/**
	 * Gets {@code Promise} from provided {@code AsyncSupplier},
	 * waits until it completes and than returns a {@code Promise<Void>}
	 */
	@NotNull
	public static Promise<Void> sequence(@NotNull AsyncSupplier<Void> promise) {
		return promise.get().toVoid();
	}

	/**
	 * Gets {@code Promise}s from provided {@code AsyncSupplier}s,
	 * end executes them consequently, discarding their results.
	 */
	@NotNull
	public static Promise<Void> sequence(@NotNull AsyncSupplier<Void> promise1, @NotNull AsyncSupplier<Void> promise2) {
		return promise1.get().then(() -> sequence(promise2));
	}

	/**
	 * @see Promises#sequence(Iterator)
	 */
	@NotNull
	@SafeVarargs
	public static Promise<Void> sequence(@NotNull AsyncSupplier<Void>... promises) {
		return sequence(asList(promises));
	}

	/**
	 * @see Promises#sequence(Iterator)
	 */
	@NotNull
	public static Promise<Void> sequence(@NotNull Iterable<? extends AsyncSupplier<Void>> promises) {
		return sequence(asPromises(promises.iterator()));
	}

	/**
	 * @see Promises#sequence(Iterator)
	 */
	@NotNull
	public static Promise<Void> sequence(@NotNull Stream<? extends AsyncSupplier<Void>> promises) {
		return sequence(asPromises(promises));
	}

	/**
	 * Calls every {@code Promise} from {@code promises} in sequence and discards
	 * their results.Returns a {@code SettablePromise} with {@code null} result as
	 * a marker when all of the {@code promises} are completed.
	 *
	 * @return {@code Promise} that completes when all {@code promises} are completed
	 */
	@NotNull
	public static Promise<Void> sequence(@NotNull Iterator<? extends Promise<Void>> promises) {
		return Promise.ofCallback(cb ->
				sequenceImpl(promises, cb));
	}

	private static void sequenceImpl(@NotNull Iterator<? extends Promise<Void>> promises, SettablePromise<Void> cb) {
		while (promises.hasNext()) {
			Promise<?> promise = promises.next();
			if (promise.isResult()) continue;
			promise.whenComplete((result, e) -> {
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
	@NotNull
	@SafeVarargs
	public static <T> Promise<T> first(AsyncSupplier<? extends T>... promises) {
		return first(Promises.isResult(), promises);
	}

	/**
	 * @see #first(AsyncSupplier[])
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@NotNull
	public static <T> Promise<T> first(@NotNull Iterable<? extends AsyncSupplier<? extends T>> promises) {
		return first(Promises.isResult(), promises);
	}

	/**
	 * @see #first(AsyncSupplier[])
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@NotNull
	public static <T> Promise<T> first(@NotNull Stream<? extends AsyncSupplier<? extends T>> promises) {
		return first(Promises.isResult(), promises);
	}

	/**
	 * @see #first(AsyncSupplier[])
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@NotNull
	public static <T> Promise<T> first(@NotNull Iterator<? extends Promise<? extends T>> promises) {
		return first(Promises.isResult(), promises);
	}

	/**
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@NotNull
	@SafeVarargs
	public static <T> Promise<T> first(@NotNull BiPredicate<? super T, ? super Throwable> predicate,
			@NotNull AsyncSupplier<? extends T>... promises) {
		return first(predicate, asList(promises));
	}

	/**
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@NotNull
	public static <T> Promise<T> first(@NotNull BiPredicate<? super T, ? super Throwable> predicate,
			@NotNull Iterable<? extends AsyncSupplier<? extends T>> promises) {
		return first(predicate, asPromises(promises));
	}

	/**
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@NotNull
	public static <T> Promise<T> first(@NotNull BiPredicate<? super T, ? super Throwable> predicate,
			@NotNull Stream<? extends AsyncSupplier<? extends T>> promises) {
		return first(predicate, asPromises(promises));
	}

	/**
	 * @param predicate filters results, consumes result of {@code Promise}
	 * @return first completed result of {@code Promise} that satisfies predicate
	 */
	@NotNull
	public static <T> Promise<T> first(@NotNull BiPredicate<? super T, ? super Throwable> predicate,
			@NotNull Iterator<? extends Promise<? extends T>> promises) {
		return Promise.ofCallback(cb ->
				firstImpl(promises, predicate, cb));
	}

	private static <T> void firstImpl(Iterator<? extends Promise<? extends T>> promises,
			@NotNull BiPredicate<? super T, ? super Throwable> predicate,
			SettablePromise<T> cb) {
		while (promises.hasNext()) {
			Promise<? extends T> nextPromise = promises.next();
			if (nextPromise.isComplete()) {
				T v = nextPromise.getResult();
				Throwable e = nextPromise.getException();
				if (predicate.test(v, e)) {
					cb.accept(v, e);
					return;
				}
				Recyclers.recycle(v);
				continue;
			}
			nextPromise.whenComplete((v, e) -> {
				if (predicate.test(v, e)) {
					cb.accept(v, e);
				} else {
					Recyclers.recycle(v);
					firstImpl(promises, predicate, cb);
				}
			});
			return;
		}
		cb.setException(NOT_ENOUGH_PROMISES_EXCEPTION);
	}

	/**
	 * Returns a {@link BiPredicate} which checks if
	 * {@code Promise} wasn't completed exceptionally.
	 */
	@NotNull
	public static <T> BiPredicate<T, Throwable> isResult() {
		return ($, e) -> e == null;
	}

	public static <T> BiPredicate<T, Throwable> isResult(Predicate<? super T> predicate) {
		return (v, e) -> e == null && predicate.test(v);
	}

	public static <T> BiPredicate<T, Throwable> isResultOrError(Predicate<? super T> predicate) {
		return (v, e) -> e != null || predicate.test(v);
	}

	public static <T> BiPredicate<T, Throwable> isResultOrError(Predicate<? super T> predicate, Predicate<? super Throwable> predicateError) {
		return (v, e) -> e == null ? predicate.test(v) : predicateError.test(e);
	}

	/**
	 * Returns a {@link BiPredicate} which checks if
	 * {@code Promise} was completed with an exception.
	 */
	@NotNull
	public static <T> BiPredicate<T, Throwable> isError() {
		return ($, e) -> e != null;
	}

	@NotNull
	public static <T> BiPredicate<T, Throwable> isError(Predicate<? super Throwable> predicate) {
		return ($, e) -> e != null && predicate.test(e);
	}

	/**
	 * Repeats the operations of provided {@code supplier} infinitely,
	 * until one of the {@code Promise}s completes exceptionally.
	 */
	@NotNull
	public static Promise<Void> repeat(@NotNull Supplier<Promise<Boolean>> supplier) {
		SettablePromise<Void> cb = new SettablePromise<>();
		repeatImpl(supplier, cb);
		return cb;
	}

	private static void repeatImpl(@NotNull Supplier<Promise<Boolean>> supplier, SettablePromise<Void> cb) {
		while (true) {
			Promise<Boolean> promise = supplier.get();
			if (promise.isResult()) {
				if (promise.getResult() == Boolean.TRUE) continue;
				cb.set(null);
				break;
			}
			promise.whenComplete((b, e) -> {
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
	 * Repeats provided {@link Function} until can pass {@link Predicate} test.
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
	public static <T> Promise<T> loop(@Nullable T seed, @NotNull Predicate<T> loopCondition, @NotNull Function<T, Promise<T>> next) {
		if (!loopCondition.test(seed)) return Promise.of(seed);
		return until(seed, next, v -> !loopCondition.test(v));
	}

	public static <T> Promise<T> until(@Nullable T seed, @NotNull Function<T, Promise<T>> next, @NotNull Predicate<T> breakCondition) {
		return Promise.ofCallback(cb ->
				untilImpl(seed, next, breakCondition, cb));
	}

	private static <T> void untilImpl(@Nullable T value, @NotNull Function<T, Promise<T>> next, @NotNull Predicate<T> breakCondition, SettablePromise<T> cb) {
		while (true) {
			Promise<T> promise = next.apply(value);
			if (promise.isResult()) {
				value = promise.getResult();
				if (breakCondition.test(value)) {
					cb.set(value);
					break;
				}
			} else {
				promise.whenComplete((newValue, e) -> {
					if (e == null) {
						if (breakCondition.test(newValue)) {
							cb.set(newValue);
						} else {
							untilImpl(newValue, next, breakCondition, cb);
						}
					} else {
						cb.setException(e);
					}
				});
				return;
			}
		}
	}

	public static <T> Promise<T> retry(AsyncSupplier<T> asyncSupplier) {
		return retry(isResult(), asyncSupplier);
	}

	public static <T> Promise<T> retry(BiPredicate<T, Throwable> breakCondition, AsyncSupplier<T> asyncSupplier) {
		return first(breakCondition, Stream.generate(() -> asyncSupplier));
	}

	public static <T> Promise<T> retry(AsyncSupplier<T> asyncSupplier, @NotNull RetryPolicy<?> retryPolicy) {
		return retry(asyncSupplier, (v, e) -> e == null, retryPolicy);
	}

	public static <T> Promise<T> retry(AsyncSupplier<T> asyncSupplier, BiPredicate<T, Throwable> breakCondition, @NotNull RetryPolicy<?> retryPolicy) {
		return Promise.ofCallback(cb ->
				retryImpl(asyncSupplier, breakCondition, (RetryPolicy<Object>) retryPolicy, null, cb));
	}

	private static <T> void retryImpl(@NotNull AsyncSupplier<? extends T> next, BiPredicate<T, Throwable> breakCondition,
			@NotNull RetryPolicy<Object> retryPolicy, Object retryState,
			SettablePromise<T> cb) {
		next.get()
				.whenComplete((v, e) -> {
					if (breakCondition.test(v, e)) {
						cb.accept(v, e);
					} else {
						Eventloop eventloop = Eventloop.getCurrentEventloop();
						long now = eventloop.currentTimeMillis();
						Object retryStateFinal = retryState != null ? retryState : retryPolicy.createRetryState();
						long nextRetryTimestamp = retryPolicy.nextRetryTimestamp(now, e, retryStateFinal);
						if (nextRetryTimestamp == 0) {
							cb.setException(e != null ? e : new StacklessException(Promises.class, "RetryPolicy: giving up " + retryState));
						} else {
							eventloop.schedule(nextRetryTimestamp,
									wrapContext(cb, () -> retryImpl(next, breakCondition, retryPolicy, retryStateFinal, cb)));
						}
					}
				});
	}

	/**
	 * Transforms a collection of {@link AsyncSupplier}
	 * {@code tasks} to a collection of {@code Promise}s.
	 */
	@NotNull
	public static <T> Iterator<Promise<T>> asPromises(@NotNull Iterator<? extends AsyncSupplier<? extends T>> tasks) {
		return transformIterator((Iterator<AsyncSupplier<T>>) tasks, AsyncSupplier::get);
	}

	/**
	 * Transforms a {@link Stream} of {@link AsyncSupplier}
	 * {@code tasks} to a collection of {@code Promise}s.
	 */
	public static <T> Iterator<Promise<T>> asPromises(@NotNull Stream<? extends AsyncSupplier<? extends T>> tasks) {
		return asPromises(tasks.iterator());
	}

	/**
	 * Transforms an {@link Iterable} of {@link AsyncSupplier}
	 * {@code tasks} to a collection of {@code Promise}s.
	 */
	public static <T> Iterator<Promise<T>> asPromises(@NotNull Iterable<? extends AsyncSupplier<? extends T>> tasks) {
		return asPromises(tasks.iterator());
	}

	/**
	 * Transforms an {@link AsyncSupplier} {@code tasks}
	 * to a collection of {@code Promise}s.
	 */
	@SafeVarargs
	public static <T> Iterator<Promise<T>> asPromises(@NotNull AsyncSupplier<? extends T>... tasks) {
		return asPromises(asIterator(tasks));
	}

	/**
	 * Allows to asynchronously reduce {@link Iterator} of {@code Promise}s
	 * into a {@code Promise} with the help of {@link Collector}. You can
	 * control the amount of concurrently running {@code Promise}.
	 * <p>
	 * This method is universal and allows to implement app-specific logic.
	 *
	 * @param collector mutable reduction operation that accumulates input
	 *                  elements into a mutable result container
	 * @param maxCalls  max amount of concurrently running {@code Promise}s
	 * @param promises  {@code Iterable} of {@code Promise}s
	 * @param <T>       type of input elements for this operation
	 * @param <A>       mutable accumulation type of the operation
	 * @param <R>       the result type of the operation
	 * @return a {@code Promise} which wraps the accumulated result
	 * of the reduction. If one of the {@code promises} completed exceptionally,
	 * a {@code Promise} with an exception will be returned.
	 */
	public static <T, A, R> Promise<R> reduce(@NotNull Collector<T, A, R> collector, int maxCalls,
			@NotNull Iterator<Promise<T>> promises) {
		return reduce(collector.supplier().get(), collector.accumulator(), collector.finisher(), maxCalls, promises);
	}

	/**
	 * @param <T>         type of input elements for this operation
	 * @param <A>         mutable accumulation type of the operation
	 * @param <R>         result type of the reduction operation
	 * @param accumulator supplier of the result
	 * @param consumer    a {@link BiConsumer} which folds a result of each of the
	 *                    completed {@code promises} into accumulator
	 * @param finisher    a {@link Function} which performs the final transformation
	 *                    from the intermediate accumulations
	 * @param maxCalls    max amount of concurrently running {@code Promise}s
	 * @param promises    {@code Iterable} of {@code Promise}s
	 * @return a {@code Promise} which wraps the accumulated result of the
	 * reduction. If one of the {@code promises} completed exceptionally, a {@code Promise}
	 * with an exception will be returned.
	 * @see Promises#reduce(Collector, int, Iterator)
	 */
	public static <T, A, R> Promise<R> reduce(A accumulator, @NotNull BiConsumer<A, T> consumer, @NotNull Function<A, R> finisher, int maxCalls, @NotNull Iterator<Promise<T>> promises) {
		AsyncAccumulator<A> asyncAccumulator = AsyncAccumulator.create(accumulator);
		for (int i = 0; promises.hasNext() && i < maxCalls; i++) {
			reduceImpl(asyncAccumulator, consumer, promises);
		}
		return asyncAccumulator.run().map(finisher);
	}

	private static <T, A> void reduceImpl(AsyncAccumulator<A> asyncAccumulator, BiConsumer<A, T> consumer, Iterator<Promise<T>> promises) {
		while (promises.hasNext()) {
			Promise<T> promise = promises.next();
			if (promise.isComplete()) {
				asyncAccumulator.addPromise(promise, consumer);
			} else {
				asyncAccumulator.addPromise(
						promise.whenResult(() -> reduceImpl(asyncAccumulator, consumer, promises)),
						consumer);
				break;
			}
		}
	}

	@Contract(pure = true)
	@NotNull
	public static <T, A, R> Function<T, Promise<R>> coalesce(@NotNull Supplier<A> argumentAccumulatorSupplier, @NotNull BiConsumer<A, T> argumentAccumulatorFn,
			@NotNull Function<A, Promise<R>> fn) {
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
	private static final class PromiseAll<T> extends NextPromise<T, Void> {
		int countdown = 1;

		@Override
		public void accept(@Nullable T result, @Nullable Throwable e) {
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

	private static final class PromiseAny<T> extends NextPromise<T, T> {
		private final BiPredicate<? super T, ? super Throwable> predicate;
		int countdown = 1;

		private PromiseAny(BiPredicate<? super T, ? super Throwable> predicate) {this.predicate = predicate;}

		@Override
		public void accept(@Nullable T result, @Nullable Throwable e) {
			if (predicate.test(result, e)) {
				if (!tryComplete(result, e)) {
					Recyclers.recycle(result);
				}
			} else {
				Recyclers.recycle(result);
				if (--countdown == 0) {
					completeExceptionally(NOT_ENOUGH_PROMISES_EXCEPTION);
				}
			}
		}

		@Override
		protected String describe() {
			return "Promises.any()";
		}
	}

	private static final class PromisesToList<T> extends AbstractPromise<List<T>> {
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
				promise.whenComplete((result, e) -> {
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
