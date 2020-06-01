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

import io.activej.async.function.AsyncSupplier;
import io.activej.common.collection.Try;
import io.activej.common.exception.AsyncTimeoutException;
import io.activej.common.exception.StacklessException;
import io.activej.common.ref.RefInt;
import io.activej.common.tuple.*;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.ScheduledRunnable;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Array;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.activej.common.Utils.nullify;
import static io.activej.common.collection.CollectionUtils.asIterator;
import static io.activej.common.collection.CollectionUtils.transformIterator;
import static io.activej.eventloop.Eventloop.getCurrentEventloop;
import static io.activej.eventloop.RunnableWithContext.wrapContext;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

/**
 * Allows to manage multiple {@link Promise}s.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
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
		return all(asIterator(promises));
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
	 * @see Promises#all(List)
	 */
	@Contract(pure = true)
	@NotNull
	public static Promise<Void> all(@NotNull Iterable<? extends Promise<?>> promises) {
		return all(promises.iterator());
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
		if (size == 1) return promises.get(0).toVoid();
		if (size == 2) return promises.get(0).both(promises.get(1));
		return all(promises.iterator());
	}

	/**
	 * Returns {@code Promise} that completes when all of the {@code promises}
	 * are completed. If at least one of the {@code promises} completes
	 * exceptionally, a {@link CompleteExceptionallyPromise} will be returned.
	 */
	@NotNull
	public static Promise<Void> all(@NotNull Iterator<? extends Promise<?>> promises) {
		if (!promises.hasNext()) return all();
		@NotNull PromiseAll<Object> resultPromise = new PromiseAll<>();
		while (promises.hasNext()) {
			Promise<?> promise = promises.next();
			if (promise.isResult()) continue;
			if (promise.isException()) return Promise.ofException(promise.getException());
			resultPromise.countdown++;
			promise.whenComplete(resultPromise);
		}
		resultPromise.countdown--;
		return resultPromise.countdown != 0 ? resultPromise : Promise.complete();
	}

	/**
	 * {@code allCompleted} should be used if you want to work only with succeeded Promise results.
	 * <p>
	 * Returns {@link Promise} that completes when all of the {@code promises} are completed.
	 * If some {@code promises} completes exceptionally,
	 * it will execute the next {@link Promise}
	 * If one or more exceptions happens,
	 * the last exception will be returned during {@code whenException} call.
	 *
	 * @since 3.0.0
	 */
	public static Promise<Void> allCompleted(@NotNull Iterator<? extends Promise<?>> promises) {
		if (!promises.hasNext()) return all();
		@NotNull PromiseAllSettled<Object> resultPromise = new PromiseAllSettled<>();
		while (promises.hasNext()) {
			Promise<?> promise = promises.next();
			if (promise.isResult()) continue;
			resultPromise.countdown++;
			promise.whenComplete(resultPromise);
		}
		resultPromise.countdown--;
		if (resultPromise.countdown != 0) {
			return resultPromise;
		}
		return Promise.complete();
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
		return Promise.ofException(new StacklessException(Promises.class, "All promises completed exceptionally"));
	}

	/**
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	@NotNull
	@SuppressWarnings("unchecked")
	public static <T> Promise<T> any(@NotNull Promise<? extends T> promise1) {
		return (Promise<T>) promise1;
	}

	/**
	 * Optimized for 2 promises.
	 *
	 * @see #any(Iterator)
	 */
	@SuppressWarnings("unchecked")
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
		return any(asIterator(promises));
	}

	/**
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> any(@NotNull Stream<? extends Promise<? extends T>> promises) {
		return any(promises.iterator());
	}

	/**
	 * @see #any(Iterator)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> any(@NotNull Iterable<? extends Promise<? extends T>> promises) {
		return any(promises.iterator());
	}

	/**
	 * @see #any(Iterator)
	 */
	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T> any(@NotNull List<? extends Promise<? extends T>> promises) {
		int size = promises.size();
		if (size == 1) return (Promise<T>) promises.get(0);
		if (size == 2) return ((Promise<T>) promises.get(0)).either(promises.get(1));
		return any(promises.iterator());
	}

	@NotNull
	public static <T> Promise<T> any(@NotNull Iterator<? extends Promise<? extends T>> promises) {
		return any(promises, $ -> {});
	}

	@NotNull
	public static <T> Promise<T> any(@NotNull Stream<? extends Promise<? extends T>> promises, @NotNull Consumer<T> cleanup) {
		return any(promises.iterator(), cleanup);
	}

	@NotNull
	public static <T> Promise<T> any(@NotNull Iterable<? extends Promise<? extends T>> promises, @NotNull Consumer<T> cleanup) {
		return any(promises.iterator(), cleanup);
	}

	/**
	 * Returns one of the first completed {@code Promise}s. Since it's
	 * async, we can't really get the FIRST completed {@code Promise}.
	 *
	 * @return one of the first completed {@code Promise}s
	 */
	@NotNull
	public static <T> Promise<T> any(@NotNull Iterator<? extends Promise<? extends T>> promises, @NotNull Consumer<T> cleanup) {
		if (!promises.hasNext()) return any();
		@NotNull PromiseAny<T> resultPromise = new PromiseAny<>();
		while (promises.hasNext()) {
			Promise<? extends T> promise = promises.next();
			if (promise.isResult()) return Promise.of(promise.getResult());
			if (promise.isException()) continue;
			resultPromise.errors++;
			promise.whenComplete((result, e) -> {
				if (e == null) {
					if (resultPromise.isComplete()) {
						cleanup.accept(result);
					} else {
						resultPromise.complete(result);
					}
				} else {
					if (--resultPromise.errors == 0) {
						resultPromise.completeExceptionally(e);
					}
				}
			});
		}
		resultPromise.errors--;
		return resultPromise.errors != 0 ? resultPromise : any();
	}

	/**
	 * Returns a {@link CompleteExceptionallyPromise} with {@link StacklessException},
	 * since this method doesn't accept any {@code Promise}s
	 *
	 * @see #some(Iterator, int)
	 */
	@Contract(pure = true)
	@NotNull
	public static Promise<?> some(int number) {
		if (number == 0) return Promise.of(emptyList());

		return Promise.ofException(new StacklessException(Promises.class,
				"There are no promises to be complete"));
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> some(@NotNull Promise<? extends T> promise, int number) {
		if (number == 0) return (Promise<List<T>>) some(number);
		if (number > 1) return Promise.ofException(new StacklessException(Promises.class,
				"There are not enough promises to be complete"));

		return promise.map(Collections::singletonList);
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> some(@NotNull Promise<? extends T> promise1,
			@NotNull Promise<? extends T> promise2,
			int number) {
		if (number == 0) return (Promise<List<T>>) some(number);
		if (number == 1) return any(promise1, promise2).map(Collections::singletonList);

		return some(asIterator(promise1, promise2), number);
	}

	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> some(@NotNull List<? extends Promise<? extends T>> promises, int number) {
		return some(promises.iterator(), number);
	}

	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> some(@NotNull Stream<? extends Promise<? extends T>> promises, int number) {
		return some(promises.iterator(), number);
	}

	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> some(@NotNull Iterable<? extends Promise<? extends T>> promises, int number) {
		return some(promises.iterator(), number);
	}

	/**
	 * Returns {@see Promise} which will have the array of first complete {@see Promise},
	 * the size of array will be the {@param number} if there will be passed the appropriate
	 * amount of {@param promises}. In case the lower amount {@param promises} then {@param number} of
	 * the return array will consist of the same {@param promises} which will be complete.
	 * <p>
	 * Provided the number is less or equal '0', the result will be the {@see Promise}
	 * and result of it is {@see CompleteExceptionallyPromise}
	 * <p>
	 * The result in the same order isn`t guaranteed
	 *
	 * @param number - amount first complete {@see Promise}
	 * @return {@see Promise} which has the array of values from completed {@param promises}
	 */
	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> some(@NotNull Iterator<? extends Promise<? extends T>> promises, int number) {
		if (number == 0 || !promises.hasNext()) return (Promise<List<T>>) some(number);

		PromiseSome<T> some = new PromiseSome<>(number);
		while (promises.hasNext()) {
			Promise<? extends T> promise = promises.next();
			if (some.isComplete()) break;
			if (promise.isResult()) {
				some.resultList.add(promise.getResult());
				if (some.isFull()) {
					return Promise.of(some.resultList);
				}
				continue;
			}
			if (promise.isException()) {
				continue;
			}
			some.activePromises++;
			promise.whenComplete(some);
		}
		some.activePromises--;

		if (some.notEnoughForTheResult()) {
			return Promise.ofException(new StacklessException(Promises.class, "There are not enough promises to be complete"));
		}

		return some;
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
		return promise1.combine(promise2, (value1, value2) -> asList(value1, value2));
	}

	/**
	 * @see Promises#toList(List)
	 */
	@SafeVarargs
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> toList(@NotNull Promise<? extends T>... promises) {
		return toList(asList(promises));
	}

	/**
	 * @see Promises#toList(List)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<List<T>> toList(@NotNull Stream<? extends Promise<? extends T>> promises) {
		List<Promise<? extends T>> list = promises.collect(Collectors.toList());
		return toList(list);
	}

	/**
	 * Reduces list of {@code Promise}s into Promise&lt;List&gt;.
	 */
	@Contract(pure = true)
	@NotNull
	@SuppressWarnings("unchecked")
	public static <T> Promise<List<T>> toList(@NotNull List<? extends Promise<? extends T>> promises) {
		int size = promises.size();
		if (size == 0) return Promise.of(Collections.emptyList());
		if (size == 1) return promises.get(0).map(Collections::singletonList);
		if (size == 2) return promises.get(0).combine(promises.get(1), Arrays::asList);

		@SuppressWarnings("unchecked") PromiseToList<T> resultPromise = new PromiseToList<>((T[]) new Object[size]);

		for (int i = 0; i < size; i++) {
			Promise<? extends T> promise = promises.get(i);
			if (promise.isResult()) {
				resultPromise.accumulator[i] = promise.getResult();
				continue;
			}
			if (promise.isException()) return Promise.ofException(promise.getException());
			int index = i;
			resultPromise.countdown++;
			promise.whenComplete((result, e) -> {
				if (e == null) {
					resultPromise.processComplete(result, index);
				} else {
					resultPromise.tryCompleteExceptionally(e);
				}
			});
		}
		return resultPromise.countdown != 0 ? resultPromise : Promise.of(((List<T>) asList(resultPromise.accumulator)));
	}

	/**
	 * Returns an array of provided {@code type} and length 0
	 * wrapped in {@code Promise}.
	 */
	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type) {
		return Promise.of((T[]) Array.newInstance(type, 0));
	}

	/**
	 * Returns an array with {@code promise1} result.
	 */
	@SuppressWarnings("unchecked")
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
	@SuppressWarnings("unchecked")
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
	@SafeVarargs
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type, @NotNull Promise<? extends T>... promises) {
		return toArray(type, asList(promises));
	}

	/**
	 * @see Promises#toArray(Class, List)
	 */
	@Contract(pure = true)
	@NotNull
	public static <T> Promise<T[]> toArray(@NotNull Class<T> type, @NotNull Stream<? extends Promise<? extends T>> promises) {
		List<Promise<? extends T>> list = promises.collect(Collectors.toList());
		return toArray(type, list);
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

		@SuppressWarnings("unchecked") PromiseToArray<T> resultPromise = new PromiseToArray<>((T[]) Array.newInstance(type, size));

		for (int i = 0; i < size; i++) {
			Promise<? extends T> promise = promises.get(i);
			if (promise.isResult()) {
				resultPromise.accumulator[i] = promise.getResult();
				continue;
			}
			if (promise.isException()) return Promise.ofException(promise.getException());
			int index = i;
			resultPromise.countdown++;
			promise.whenComplete((result, e) -> {
				if (e == null) {
					resultPromise.processComplete(result, index);
				} else {
					resultPromise.tryCompleteExceptionally(e);
				}
			});
		}
		return resultPromise.countdown != 0 ? resultPromise : Promise.of(resultPromise.accumulator);
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

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3, R> Promise<R> toTuple(@NotNull TupleConstructor3<T1, T2, T3, R> constructor,
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3) {
		return toList(promise1, promise2, promise3)
				.map(list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2)));
	}

	@SuppressWarnings("unchecked")
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

	@SuppressWarnings("unchecked")
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

	@SuppressWarnings("unchecked")
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
		return promise1.map((Function<T1, Tuple1<T1>>) Tuple1::new);
	}

	@Contract(pure = true)
	@NotNull
	public static <T1, T2> Promise<Tuple2<T1, T2>> toTuple(@NotNull Promise<? extends T1> promise1, @NotNull Promise<? extends T2> promise2) {
		return promise1.combine(promise2, (BiFunction<T1, T2, Tuple2<T1, T2>>) Tuple2::new);
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public static <T1, T2, T3> Promise<Tuple3<T1, T2, T3>> toTuple(
			@NotNull Promise<? extends T1> promise1,
			@NotNull Promise<? extends T2> promise2,
			@NotNull Promise<? extends T3> promise3) {
		return toList(promise1, promise2, promise3)
				.map(list -> new Tuple3<>((T1) list.get(0), (T2) list.get(1), (T3) list.get(2)));
	}

	@SuppressWarnings("unchecked")
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

	@SuppressWarnings("unchecked")
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

	@SuppressWarnings("unchecked")
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
		return sequence(asPromises(promises.iterator()));
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
	public static <T> Promise<T> firstSuccessful(AsyncSupplier<? extends T>... promises) {
		return first(isResult(), promises);
	}

	/**
	 * @see #firstSuccessful(AsyncSupplier[])
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@NotNull
	public static <T> Promise<T> firstSuccessful(@NotNull Iterable<? extends AsyncSupplier<? extends T>> promises) {
		return first(isResult(), promises);
	}

	/**
	 * @see #firstSuccessful(AsyncSupplier[])
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@NotNull
	public static <T> Promise<T> firstSuccessful(@NotNull Stream<? extends AsyncSupplier<? extends T>> promises) {
		return first(isResult(), promises);
	}

	/**
	 * @see #firstSuccessful(AsyncSupplier[])
	 * @see Promises#first(BiPredicate, Iterator)
	 */
	@NotNull
	public static <T> Promise<T> firstSuccessful(@NotNull Iterator<? extends Promise<? extends T>> promises) {
		return first(isResult(), promises);
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
				continue;
			}
			nextPromise.whenComplete((v, e) -> {
				if (predicate.test(v, e)) {
					cb.accept(v, e);
					return;
				}
				firstImpl(promises, predicate, cb);
			});
			return;
		}
		cb.setException(new StacklessException(Promises.class, "No promise result met the condition"));
	}

	/**
	 * Returns a {@link BiPredicate} which checks if
	 * {@code Promise} wasn't completed exceptionally.
	 */
	@Contract(value = " -> new", pure = true)
	@NotNull
	public static <T> BiPredicate<T, Throwable> isResult() {
		return ($, e) -> e == null;
	}

	/**
	 * Returns a {@link BiPredicate} which checks if
	 * {@code Promise} was completed with an exception.
	 */
	@Contract(value = " -> new", pure = true)
	@NotNull
	public static <T> BiPredicate<T, Throwable> isError() {
		return ($, e) -> e != null;
	}

	/**
	 * Repeats the operations of provided {@code supplier} infinitely,
	 * until one of the {@code Promise}s completes exceptionally.
	 */
	@NotNull
	public static Promise<Void> repeat(@NotNull Supplier<Promise<Void>> supplier) {
		return Promise.ofCallback(cb ->
				repeatImpl(supplier, cb));
	}

	private static void repeatImpl(@NotNull Supplier<Promise<Void>> supplier, SettablePromise<Void> cb) {
		while (true) {
			Promise<Void> promise = supplier.get();
			if (promise.isResult()) {
				continue;
			}
			promise.whenComplete(($, e) -> {
				if (e == null) {
					repeatImpl(supplier, cb);
				} else {
					cb.setException(e);
				}
			});
			return;
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
		return retry(asyncSupplier, (v, e) -> e == null);
	}

	public static <T> Promise<T> retry(AsyncSupplier<T> asyncSupplier, BiPredicate<T, Throwable> breakCondition) {
		return Promise.ofCallback(cb -> retryImpl(asyncSupplier, breakCondition, cb));
	}

	static <T> void retryImpl(AsyncSupplier<T> next, BiPredicate<T, Throwable> breakCondition, SettablePromise<T> cb) {
		while (true) {
			Promise<T> promise = next.get();
			if (promise.isComplete()) {
				T v = promise.getResult();
				Throwable e = promise.getException();
				if (breakCondition.test(v, e)) {
					cb.accept(v, e);
					return;
				}
				continue;
			}
			promise.whenComplete((v, e) -> {
				if (breakCondition.test(v, e)) {
					cb.accept(v, e);
				} else {
					retryImpl(next, breakCondition, cb);
				}
			});
			break;
		}
	}

	public static <T> Promise<T> retry(AsyncSupplier<T> asyncSupplier, @NotNull RetryPolicy<?> retryPolicy) {
		return retry(asyncSupplier, (v, e) -> e == null, retryPolicy);
	}

	public static <T> Promise<T> retry(AsyncSupplier<T> asyncSupplier, BiPredicate<T, Throwable> breakCondition, @NotNull RetryPolicy<?> retryPolicy) {
		//noinspection unchecked
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
	@SuppressWarnings("unchecked")
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
		return asPromises(asList(tasks));
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
		return reduce(promises, maxCalls, collector.supplier().get(), collector.accumulator(), collector.finisher());
	}

	/**
	 * @param promises    {@code Iterable} of {@code Promise}s
	 * @param accumulator supplier of the result
	 * @param maxCalls    max amount of concurrently running {@code Promise}s
	 * @param consumer    a {@link BiConsumer} which folds a result of each of the
	 *                    completed {@code promises} into accumulator
	 * @param finisher    a {@link Function} which performs the final transformation
	 *                    from the intermediate accumulations
	 * @param <T>         type of input elements for this operation
	 * @param <A>         mutable accumulation type of the operation
	 * @param <R>         result type of the reduction operation
	 * @return a {@code Promise} which wraps the accumulated result of the
	 * reduction. If one of the {@code promises} completed exceptionally, a {@code Promise}
	 * with an exception will be returned.
	 * @see #reduce(Collector, int, Iterator)
	 */
	public static <T, A, R> Promise<R> reduce(@NotNull Iterator<Promise<T>> promises, int maxCalls,
			A accumulator,
			@NotNull BiConsumer<A, T> consumer,
			@NotNull Function<A, R> finisher) {
		return Promise.ofCallback(cb ->
				reduceImpl(promises, maxCalls, new RefInt(0),
						accumulator, consumer, finisher, cb));
	}

	private static <T, A, R> void reduceImpl(Iterator<Promise<T>> promises, int maxCalls, RefInt calls,
			A accumulator, BiConsumer<A, T> consumer, Function<A, R> finisher,
			SettablePromise<R> cb) {
		calls.inc();
		while (promises.hasNext() && calls.get() <= maxCalls + 1) {
			Promise<T> promise = promises.next();
			if (cb.isComplete()) return;
			if (promise.isComplete()) {
				if (promise.isResult()) {
					consumer.accept(accumulator, promise.getResult());
					continue;
				} else {
					cb.setException(promise.getException());
					return;
				}
			}
			calls.inc();
			promise.whenComplete((v, e) -> {
				calls.dec();
				if (cb.isComplete()) {
					return;
				}
				if (e == null) {
					consumer.accept(accumulator, v);
					reduceImpl(promises, maxCalls, calls,
							accumulator, consumer, finisher, cb);
				} else {
					cb.setException(e);
				}
			});
		}
		calls.dec();
		if (calls.get() == 0) {
			R result = finisher.apply(accumulator);
			cb.set(result);
		}
	}

	/**
	 * Allows to asynchronously reduce {@link Iterator} of {@code Promise}s
	 * into a {@code Promise} with the help of {@link Collector}. You can
	 * control the amount of concurrently running {@code promises} and explicitly
	 * process exceptions and intermediate results.
	 * <p>
	 * The main feature of this method is that you can set up {@code consumer}
	 * for different use cases, for example:
	 * <ul>
	 * <li> If one of the {@code promises} completes exceptionally, reduction
	 * will stop without waiting for all of the {@code promises} to be completed.
	 * A {@code Promise} with exception will be returned.
	 * <li> If one of the {@code promises} finishes with needed result, reduction
	 * will stop without waiting for all of the {@code promises} to be completed.
	 * <li> If a needed result accumulates before all of the {@code promises} run,
	 * reduction will stop without waiting for all of the {@code promises} to be completed.
	 * </ul>
	 * <p>
	 * To implement the use cases, you need to set up the provided {@code consumer}'s
	 * {@link BiFunction#apply(Object, Object)} function. This function will be applied
	 * to each of the completed {@code promises} and corresponding accumulated result.
	 * <p>
	 * When {@code apply} returns {@code null}, nothing happens and reduction continues.
	 * When {@link Try} with any result or exception is returned, the reduction stops without
	 * waiting for all of the {@code promises} to be completed and {@code Promise} with
	 * {@code Try}'s result or exception is returned
	 * .
	 *
	 * @param promises    {@code Iterable} of {@code Promise}s
	 * @param maxCalls    {@link ToIntFunction} which calculates max amount of concurrently
	 *                    running {@code Promise}s based on the {@code accumulator} value
	 * @param accumulator mutable supplier of the result
	 * @param consumer    a {@link BiConsumer} which folds a result of each of the completed
	 *                    {@code promises} into accumulator for further processing
	 * @param finisher    a {@link Function} which performs the final transformation
	 *                    from the intermediate accumulations
	 * @param recycler    processes results of those {@code promises} which were
	 *                    completed after result of the reduction was returned
	 * @param <T>         type of input elements for this operation
	 * @param <A>         mutable accumulation type of the operation
	 * @param <R>         result type of the reduction operation
	 * @return a {@code Promise} which wraps accumulated result or exception.
	 */
	public static <T, A, R> Promise<R> reduceEx(@NotNull Iterator<Promise<T>> promises, @NotNull ToIntFunction<A> maxCalls,
			A accumulator,
			@NotNull BiFunction<A, Try<T>, @Nullable Try<R>> consumer,
			@NotNull Function<A, @NotNull Try<R>> finisher,
			@Nullable Consumer<T> recycler) {
		return Promise.ofCallback(cb ->
				reduceExImpl(promises, maxCalls, new RefInt(0),
						accumulator, consumer, finisher, recycler, cb));
	}

	private static <T, A, R> void reduceExImpl(Iterator<Promise<T>> promises, ToIntFunction<A> maxCalls, RefInt calls,
			A accumulator, BiFunction<A, Try<T>, Try<R>> consumer, Function<A, @NotNull Try<R>> finisher, @Nullable Consumer<T> recycler,
			SettablePromise<R> cb) {
		calls.inc();
		while (promises.hasNext() && calls.get() <= maxCalls.applyAsInt(accumulator) + 1) {
			Promise<T> promise = promises.next();
			if (cb.isComplete()) return;
			if (promise.isComplete()) {
				@Nullable Try<R> maybeResult = consumer.apply(accumulator, promise.getTry());
				if (maybeResult != null) {
					cb.accept(maybeResult.getOrNull(), maybeResult.getExceptionOrNull());
					return;
				}
				continue;
			}
			calls.inc();
			promise.whenComplete((v, e) -> {
				calls.dec();
				if (cb.isComplete()) {
					if (recycler != null) recycler.accept(v);
					return;
				}
				@Nullable Try<R> maybeResult = consumer.apply(accumulator, Try.of(v, e));
				if (maybeResult != null) {
					cb.accept(maybeResult.getOrNull(), maybeResult.getExceptionOrNull());
				} else {
					reduceExImpl(promises, maxCalls, calls,
							accumulator, consumer, finisher, recycler, cb);
				}
			});
		}
		calls.dec();
		if (calls.get() == 0) {
			@NotNull Try<R> result = finisher.apply(accumulator);
			if (result.isSuccess()) {
				cb.set(result.get());
			} else {
				cb.setException(result.getException());
			}
		}
	}

	// region helper classes
	private static final class PromiseAll<T> extends NextPromise<T, Void> {
		int countdown = 1;

		@Override
		public void accept(@Nullable T result, @Nullable Throwable e) {
			if (e == null) {
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

	private static final class PromiseAllSettled<T> extends NextPromise<T, Void> {
		int countdown;
		@Nullable Throwable lastException;

		@Override
		public String describe() {
			return "PromiseAllSettled{" +
					"countdown=" + countdown +
					", lastException=" + lastException +
					'}';
		}

		@Override
		public void accept(T result, @Nullable Throwable e) {
			if (e != null) {
				lastException = e;
			}
			if (--countdown == 0) {
				if (lastException == null) {
					complete(null);
				} else {
					completeExceptionally(lastException);
				}
			}
		}
	}

	private static final class PromiseAny<T> extends NextPromise<T, T> {
		int errors = 1;

		@Override
		public void accept(@Nullable T result, @Nullable Throwable e) {
			if (e == null) {
				tryComplete(result);
			} else {
				if (--errors == 0) {
					completeExceptionally(e);
				}
			}
		}

		@Override
		protected String describe() {
			return "Promises.any()";
		}
	}

	private static final class PromiseSome<T> extends NextPromise<T, List<T>> {
		final List<T> resultList;
		final int expectedSize;
		int activePromises = 1;

		PromiseSome(int expectedSize) {
			this.expectedSize = expectedSize;
			this.resultList = new ArrayList<>(expectedSize);
		}

		@Override
		public void accept(@Nullable T result, @Nullable Throwable e) {
			activePromises--;
			if (isComplete()) {
				return;
			}
			if (e == null) {
				resultList.add(result);
				if (isFull()) {
					complete(resultList);
				}
			} else {
				if (notEnoughForTheResult()) {
					completeExceptionally(new StacklessException(Promises.class, "There are not enough promises to be complete"));
				}
			}
		}

		@Override
		protected String describe() {
			return "Promises.some(" + expectedSize + ")";
		}

		boolean isFull() {
			return resultList.size() == expectedSize;
		}

		boolean notEnoughForTheResult() {
			return activePromises + resultList.size() < expectedSize;
		}
	}

	private static final class PromiseToArray<T> extends NextPromise<T, T[]> {
		final T[] accumulator;
		int countdown;

		private PromiseToArray(@NotNull T[] accumulator) {
			this.accumulator = accumulator;
		}

		void processComplete(@Nullable T result, int i) {
			if (isComplete()) {
				return;
			}
			accumulator[i] = result;
			if (--countdown == 0) {
				complete(this.accumulator);
			}
		}

		@Override
		public void accept(@Nullable T result, @Nullable Throwable e) {
			if (e == null) {
				processComplete(result, 0);
			} else {
				tryCompleteExceptionally(e);
			}
		}

		@Override
		protected String describe() {
			return "Promises.toArray()";
		}
	}

	private static final class PromiseToList<T> extends NextPromise<T, List<T>> {
		final Object[] accumulator;
		int countdown;

		private PromiseToList(@NotNull T[] accumulator) {
			this.accumulator = accumulator;
		}

		@SuppressWarnings("unchecked")
		void processComplete(@Nullable T result, int i) {
			if (isComplete()) {
				return;
			}
			accumulator[i] = result;
			if (--countdown == 0) {
				complete((List<T>) asList(this.accumulator));
			}
		}

		@Override
		public void accept(@Nullable T result, @Nullable Throwable e) {
			if (e == null) {
				processComplete(result, 0);
			} else {
				tryCompleteExceptionally(e);
			}
		}

		@Override
		protected String describe() {
			return "Promises.toList()";
		}
	}

	// endregion

	@Contract(pure = true)
	@NotNull
	public static <T, A, R> Function<T, Promise<R>> coalesce(@NotNull Supplier<A> argumentAccumulatorSupplier, @NotNull BiConsumer<A, T> argumentAccumulatorFn,
			@NotNull Function<A, Promise<R>> fn) {
		return new CoalesceImpl<>(argumentAccumulatorSupplier, argumentAccumulatorFn, fn);
	}

	private static class CoalesceImpl<T, R, A> implements Function<T, Promise<R>> {
		@NotNull
		private final Supplier<A> argumentAccumulatorSupplier;
		@NotNull
		private final BiConsumer<A, T> argumentAccumulatorFn;
		@NotNull
		private final Function<A, Promise<R>> fn;

		boolean asyncRunning;
		@Nullable
		private SettablePromise<R> nextPromise;
		private A argumentAccumulator;

		public CoalesceImpl(@NotNull Supplier<A> argumentAccumulatorSupplier, @NotNull BiConsumer<A, T> argumentAccumulatorFn, @NotNull Function<A, Promise<R>> fn) {
			this.argumentAccumulatorSupplier = argumentAccumulatorSupplier;
			this.argumentAccumulatorFn = argumentAccumulatorFn;
			this.fn = fn;
		}

		@Override
		public Promise<R> apply(T parameters) {
			if (!asyncRunning) {
				assert nextPromise == null && this.argumentAccumulator == null;
				A argumentAccumulator = argumentAccumulatorSupplier.get();
				argumentAccumulatorFn.accept(argumentAccumulator, parameters);
				Promise<R> promise = fn.apply(argumentAccumulator);
				asyncRunning = true;
				SettablePromise<R> result = new SettablePromise<>();
				promise.whenComplete((v, e) -> {
					result.accept(v, e);
					asyncRunning = false;
					processNext();
				});
				return result;
			}
			if (nextPromise == null) {
				nextPromise = new SettablePromise<>();
				this.argumentAccumulator = argumentAccumulatorSupplier.get();
			}
			argumentAccumulatorFn.accept(this.argumentAccumulator, parameters);
			return nextPromise;
		}

		void processNext() {
			while (this.nextPromise != null) {
				SettablePromise<R> nextPromise = this.nextPromise;
				A argumentAccumulator = this.argumentAccumulator;
				this.nextPromise = null;
				this.argumentAccumulator = null;
				asyncRunning = true;
				Promise<? extends R> promise = fn.apply(argumentAccumulator);
				if (promise.isComplete()) {
					promise.whenComplete(nextPromise);
					asyncRunning = false;
					continue;
				}
				promise.whenComplete((result, e) -> {
					nextPromise.accept(result, e);
					asyncRunning = false;
					processNext();
				});
				break;
			}
		}
	}
}
