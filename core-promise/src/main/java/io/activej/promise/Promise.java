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

import io.activej.async.callback.AsyncComputation;
import io.activej.async.callback.Callback;
import io.activej.common.collection.Try;
import io.activej.common.function.*;
import io.activej.eventloop.Eventloop;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.exception.FatalErrorHandlers.getExceptionOrThrowError;
import static io.activej.common.exception.FatalErrorHandlers.handleError;
import static io.activej.eventloop.util.RunnableWithContext.wrapContext;

/**
 * Replacement of default Java {@link CompletionStage} interface with
 * optimized design, which allows handling different scenarios more
 * efficiently.
 * <p>
 * Each promise represents some sort of operations executed
 * after the previous {@code Promise} completes.
 * <p>
 * {@code Promise} can complete either successfully with a result
 * which will be wrapped inside the {@code Promise} or exceptionally,
 * returning a new {@link CompletePromise} or {@link CompleteExceptionallyPromise}
 * respectively.
 * <p>
 * {@link SettablePromise} allows to create a root for chain of {@code Promise}s.
 * <p>
 *
 * @see CompletionStage
 */
public interface Promise<T> extends Promisable<T>, AsyncComputation<T> {
	/**
	 * Creates successfully completed {@code Promise}
	 */
	@SuppressWarnings("unchecked")
	static @NotNull Promise<Void> complete() {
		return (Promise<Void>) CompleteNullPromise.INSTANCE;
	}

	/**
	 * Creates successfully completed {@code Promise}.
	 *
	 * @param value result of Promise. If value is {@code null},
	 *              returns {@link CompleteNullPromise}, otherwise
	 *              {@link CompleteResultPromise}
	 */
	static <T> @NotNull Promise<T> of(@Nullable T value) {
		return value != null ? new CompleteResultPromise<>(value) : CompleteNullPromise.instance();
	}

	/**
	 * Creates an exceptionally completed {@code Promise}.
	 *
	 * @param e Exception
	 */
	static <T> @NotNull Promise<T> ofException(@NotNull Exception e) {
		return new CompleteExceptionallyPromise<>(e);
	}

	/**
	 * Creates and returns a new {@link SettablePromise}
	 * that is accepted by the provided {@link ConsumerEx} of
	 * {@link SettablePromise}
	 */
	static <T> @NotNull Promise<T> ofCallback(@NotNull ConsumerEx<@NotNull SettablePromise<T>> callbackConsumer) {
		SettablePromise<T> cb = new SettablePromise<>();
		try {
			callbackConsumer.accept(cb);
		} catch (Exception ex) {
			handleError(ex, callbackConsumer);
			return Promise.ofException(ex);
		}
		return cb;
	}

	/**
	 * Creates a new {@code Promise} of the given value
	 *
	 * @see #ofOptional(Optional, Supplier)
	 */
	@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
	static <T> @NotNull Promise<T> ofOptional(@NotNull Optional<T> optional) {
		return ofOptional(optional, NoSuchElementException::new);
	}

	/**
	 * Creates a new {@code Promise} of the given value.
	 * If {@code Optional} doesn't equal {@code null}, a
	 * {@code Promise} of {@code optional} contained value
	 * will be created. Otherwise, a {@code Promise} with
	 * {@code errorSupplier} exception will be created.
	 *
	 * @return {@link CompletePromise} if the optional value
	 * doesn't equal {@code null}, otherwise
	 * {@link CompleteExceptionallyPromise} with
	 * {@code errorSupplier} exception.
	 */
	@SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "OptionalIsPresent"})
	static <T> @NotNull Promise<T> ofOptional(@NotNull Optional<T> optional, @NotNull Supplier<? extends Exception> errorSupplier) {
		if (optional.isPresent()) return Promise.of(optional.get());
		return Promise.ofException(errorSupplier.get());
	}

	/**
	 * Creates a completed {@code Promise} from {@code T value} and
	 * {@code Exception e} parameters, any of them can be {@code null}.
	 * Useful for {@link #then(BiFunctionEx)} passthroughs
	 * (for example, when mapping specific exceptions).
	 *
	 * @param value value to wrap when exception is null
	 * @param e     possibly-null exception, determines type of promise completion
	 */
	static <T> @NotNull Promise<T> of(@Nullable T value, @Nullable Exception e) {
		checkArgument(!(value != null && e != null), "Either value or exception should be 'null'");
		return e == null ? of(value) : ofException(e);
	}

	/**
	 * Returns a new {@link CompletePromise} or {@link CompleteExceptionallyPromise}
	 * based on the provided {@link Try}.
	 */
	static <T> @NotNull Promise<T> ofTry(@NotNull Try<T> t) {
		return t.reduce(Promise::of, Promise::ofException);
	}

	/**
	 * Creates a {@code Promise} wrapper around default
	 * Java {@code CompletableFuture} and runs it immediately.
	 *
	 * @return a new {@code Promise} with a result of the given future
	 */
	static <T> @NotNull Promise<T> ofFuture(@NotNull CompletableFuture<? extends T> future) {
		return ofCompletionStage(future);
	}

	/**
	 * Wraps Java {@link CompletionStage} in a {@code Promise}, running it in current eventloop.
	 *
	 * @param completionStage completion stage itself
	 * @return result of the given completionStage wrapped in a {@code Promise}
	 */
	static <T> @NotNull Promise<T> ofCompletionStage(CompletionStage<? extends T> completionStage) {
		return ofCallback(cb -> {
			Eventloop eventloop = Eventloop.getCurrentEventloop();
			eventloop.startExternalTask();
			completionStage.whenCompleteAsync((result, throwable) -> {
				eventloop.execute(wrapContext(cb, () -> {
					if (throwable == null) {
						cb.accept(result, null);
					} else {
						Exception e = getExceptionOrThrowError(throwable);
						handleError(e, cb);
						cb.accept(null, e);
					}
				}));
				eventloop.completeExternalTask();
			});
		});
	}

	/**
	 * Wraps Java {@code Future} in a {@code Promise} running it with given {@link Executor}.
	 *
	 * @param executor executor to execute the future concurrently
	 * @param future   the future itself
	 * @return a new {@code Promise} of the future result
	 */
	static <T> @NotNull Promise<T> ofFuture(@NotNull Executor executor, @NotNull Future<? extends T> future) {
		return ofCallback(cb -> {
			Eventloop eventloop = Eventloop.getCurrentEventloop();
			eventloop.startExternalTask();
			try {
				executor.execute(() -> {
					try {
						T value = future.get();
						eventloop.execute(wrapContext(cb, () -> cb.set(value)));
					} catch (ExecutionException ex) {
						eventloop.execute(wrapContext(cb, () -> {
							Exception e = getExceptionOrThrowError(ex.getCause());
							handleError(e, cb);
							cb.setException(e);
						}));
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						eventloop.execute(wrapContext(cb, () -> cb.setException(e)));
					} catch (CancellationException e) {
						eventloop.execute(wrapContext(cb, () -> cb.setException(e)));
					} catch (Throwable throwable) {
						eventloop.execute(wrapContext(cb, () -> {
							Exception e = getExceptionOrThrowError(throwable);
							handleError(e, cb);
							cb.setException(e);
						}));
					} finally {
						eventloop.completeExternalTask();
					}
				});
			} catch (RejectedExecutionException e) {
				eventloop.completeExternalTask();
				cb.setException(e);
			}
		});
	}

	/**
	 * Runs some task in another thread (executed by a given {@code Executor})
	 * and returns a {@code Promise} for it. Also manages external task count
	 * for current eventloop, so it won't shut down until the task is complete.
	 *
	 * @param executor executor to execute the task concurrently
	 * @param supplier the task itself
	 * @return {@code Promise} for the given task
	 */
	static <T> Promise<T> ofBlocking(@NotNull Executor executor, @NotNull SupplierEx<? extends T> supplier) {
		return ofCallback(cb -> {
			Eventloop eventloop = Eventloop.getCurrentEventloop();
			eventloop.startExternalTask();
			try {
				executor.execute(() -> {
					try {
						T result = supplier.get();
						eventloop.execute(wrapContext(cb, () -> cb.set(result)));
					} catch (Throwable throwable) {
						eventloop.execute(wrapContext(cb, () -> {
							Exception e = getExceptionOrThrowError(throwable);
							handleError(e, cb);
							cb.setException(e);
						}));
					} finally {
						eventloop.completeExternalTask();
					}
				});
			} catch (RejectedExecutionException e) {
				eventloop.completeExternalTask();
				cb.setException(e);
			}
		});
	}

	/**
	 * Same as {@link #ofBlocking(Executor, SupplierEx)}, but without a result
	 * (returned {@code Promise} is only a marker of completion).
	 */
	static @NotNull Promise<Void> ofBlocking(@NotNull Executor executor, @NotNull RunnableEx runnable) {
		return Promise.ofBlocking(executor, () -> {
			runnable.run();
			return null;
		});
	}

	@Override
	default Promise<T> promise() {
		return this;
	}

	@Contract(pure = true)
	default boolean isComplete() {
		return isResult() || isException();
	}

	@Contract(pure = true)
	boolean isResult();

	@Contract(pure = true)
	boolean isException();

	@Contract(pure = true)
	T getResult();

	@Contract(pure = true)
	Exception getException();

	@Contract(pure = true)
	Try<T> getTry();

	/**
	 * Ensures that {@code Promise} completes asynchronously:
	 * if this {@code Promise} is already completed, its
	 * completion will be posted to the next eventloop tick.
	 * Otherwise, does nothing.
	 */
	@Contract(pure = true)
	@NotNull Promise<T> async();

	/**
	 * Executes given {@link NextPromise} after execution
	 * of this {@code Promise} completes.
	 *
	 * @param promise given {@link NextPromise}
	 * @param <U>     type of result
	 * @return subscribed {@code Promise}
	 */
	@Contract("_ -> param1")
	<U> @NotNull Promise<U> next(@NotNull NextPromise<T, U> promise);

	/**
	 * Returns a new {@code Promise} which is obtained by mapping
	 * a result of {@code this} promise to some other value.
	 * If {@code this} promise is completed exceptionally, a mapping
	 * function will not be applied.
	 *
	 * <p>
	 * A mapping function may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param fn a function to map the result of this {@code Promise}
	 *           to a new value
	 * @return new {@code Promise} whose result is the result of function
	 * applied to the result of {@code this} promise
	 * @see CompletionStage#thenApply(Function)
	 */
	<U> @NotNull Promise<U> map(@NotNull FunctionEx<? super T, ? extends U> fn);

	/**
	 * Returns a new {@code Promise} which is obtained by mapping
	 * a result of {@code this} promise to some other value using one of
	 * provided mapping functions.
	 * <p>
	 * If predicate returns {@code true}, the first mapping function is used,
	 * otherwise the second mapping function is used.
	 * <p>
	 * If {@code this} promise is completed exceptionally, no mapping
	 * function will be applied.
	 *
	 * <p>
	 * A mapping functions may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param predicate a predicate that tests the result of a promise
	 *                  to decide which one of the two mapping functions
	 *                  will be applied to the promise result
	 * @param fn        a function to map the result of this {@code Promise}
	 *                  to a new value if a predicate returns {@code true}
	 * @param elseFn    a function to map the result of this {@code Promise}
	 *                  to a new value if a predicate returns {@code false}
	 * @return new {@code Promise} whose result is the result of one of two
	 * mapping functions applied to the result of {@code this} promise
	 */
	default <U> @NotNull Promise<U> mapIfElse(@NotNull Predicate<? super T> predicate,
			@NotNull FunctionEx<? super T, ? extends U> fn, @NotNull FunctionEx<? super T, ? extends U> elseFn) {
		return map(t -> predicate.test(t) ? fn.apply(t) : elseFn.apply(t));
	}

	/**
	 * Returns a new {@code Promise} which is obtained by mapping
	 * a result of {@code this} promise to some other value of the same type.
	 * <p>
	 * If predicate returns {@code true}, the mapping function is used,
	 * otherwise the result is returned as-is.
	 * <p>
	 * If {@code this} promise is completed exceptionally, a mapping
	 * function will not be applied.
	 *
	 * <p>
	 * A mapping function may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param predicate a predicate that tests the result of a promise
	 *                  to decide whether a mapping function
	 *                  will be applied to the promise result
	 * @param fn        a function to map the result of this {@code Promise}
	 *                  to a new value if a predicate returns {@code true}
	 * @return new {@code Promise} whose result is either the original result of a promise
	 * or a mapped result of {@code this} promise
	 */
	default @NotNull Promise<T> mapIf(@NotNull Predicate<? super T> predicate,
			@NotNull FunctionEx<? super T, ? extends T> fn) {
		return mapIfElse(predicate, fn, FunctionEx.identity());
	}

	/**
	 * Returns a new {@code Promise} which is obtained by supplying
	 * a new result in case a promise result is {@code null}
	 * <p>
	 * If a promise result is not {@code null}, the result is returned as-is
	 * <p>
	 * If {@code this} promise is completed exceptionally, a supplier will not be called.
	 *
	 * <p>
	 * A supplier may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param supplier a supplier of a new promise result in case an original
	 *                 promise result is {@code null}
	 * @return new {@code Promise} where {@code null} result is replaced with a
	 * result supplied by a given supplier
	 */
	default @NotNull Promise<T> mapIfNull(@NotNull SupplierEx<? extends T> supplier) {
		return mapIf(Objects::isNull, $ -> supplier.get());
	}

	/**
	 * Returns a new {@code Promise} which is obtained by mapping
	 * a non-{@code null} result of {@code this} promise to some other value.
	 * <p>
	 * If a promise result is {@code null}, the mapping function is not used
	 * and result is returned as-is
	 * <p>
	 * If {@code this} promise is completed exceptionally, a mapping
	 * function will not be applied.
	 *
	 * <p>
	 * A mapping function may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param fn a function to map the result of this {@code Promise}
	 *           to a new value if a result of {@code this} promise is {@code null}
	 * @return new {@code Promise} whose result is either {@code null} or a mapped result
	 */
	default <U> @NotNull Promise<U> mapIfNonNull(@NotNull FunctionEx<? super @NotNull T, ? extends U> fn) {
		return mapIfElse(Objects::nonNull, fn, $ -> null);
	}

	/**
	 * Returns a new {@code Promise} which is obtained by mapping
	 * a result and an exception of {@code this} promise to some other value.
	 * If {@code this} promise is completed exceptionally, an exception
	 * passed to a mapping function is guaranteed to be not null.
	 *
	 * <p>
	 * A {bi function} may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param fn a function to map a result and
	 *           an exception of {@code this} promise to some other value
	 * @return new {@code Promise} whose result is the result of function
	 * applied to the result and exception of {@code this} promise
	 */
	<U> @NotNull Promise<U> map(@NotNull BiFunctionEx<? super T, @Nullable Exception, ? extends U> fn);

	/**
	 * Returns a new {@code Promise} which is obtained by mapping
	 * either a result or an exception of {@code this} promise to some other values.
	 * If {@code this} promise is completed successfully, the first function will be called mapping the
	 * result to some other value. Otherwise, second function will be called mapping the exception
	 * to some other value.
	 *
	 * <p>
	 * Each function may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param fn          a function to map a result of {@code this} promise to some other value
	 * @param exceptionFn a function to map an exception of {@code this} promise to some other value
	 * @return new {@code Promise} whose result is the result of either first or second
	 * function applied either to a result or an exception of {@code this} promise.
	 */
	default <U> @NotNull Promise<U> map(@NotNull FunctionEx<? super T, ? extends U> fn, @NotNull FunctionEx<@NotNull Exception, ? extends U> exceptionFn) {
		return map((v, e) -> e == null ? fn.apply(v) : exceptionFn.apply(e));
	}

	/**
	 * Returns a new {@code Promise} which is obtained by mapping
	 * an exception of {@code this} promise to some other exception.
	 * The mapping function will be called only if {@code this} promise
	 * completes exceptionally
	 *
	 * <p>
	 * A mapping function may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param exceptionFn a function to map an exception of {@code this} promise to some other value
	 * @return new {@code Promise} whose result is the result of a mapping
	 * function applied either to an exception of {@code this} promise.
	 */
	default @NotNull Promise<T> mapException(@NotNull FunctionEx<@NotNull Exception, Exception> exceptionFn) {
		return then(Promise::of, e -> Promise.ofException(exceptionFn.apply(e)));
	}

	/**
	 * Returns a new {@code Promise} which is obtained by mapping
	 * an exception of {@code this} promise to some other exception.
	 * The mapping function will be called only if {@code this} promise
	 * completes with an exception that satisfies a given {@link Predicate}
	 *
	 * <p>
	 * A mapping function may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param predicate   a predicate that tests whether to map a received exception
	 * @param exceptionFn a function to map an exception of {@code this} promise to some other value
	 * @return new {@code Promise} whose result is the result of a mapping
	 * function applied either to an exception of {@code this} promise.
	 */
	default @NotNull Promise<T> mapException(@NotNull Predicate<Exception> predicate,
			@NotNull FunctionEx<@NotNull Exception, @NotNull Exception> exceptionFn) {
		return mapException(e -> predicate.test(e) ? exceptionFn.apply(e) : e);
	}

	/**
	 * Returns a new {@code Promise} which is obtained by mapping
	 * an exception of {@code this} promise to some other exception.
	 * The mapping function will be called only if {@code this} promise
	 * completes with an exception that is an instance of a given {@link Class}
	 *
	 * <p>
	 * A mapping function may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param clazz       an exception class that exception is tested against. A mapping function is applied
	 *                    only if an exception of {@code this} promise is an instance of the specified class
	 * @param exceptionFn a function to map an exception of {@code this} promise to some other value
	 * @return new {@code Promise} whose result is the result of a mapping
	 * function applied either to an exception of {@code this} promise.
	 */
	default @NotNull Promise<T> mapException(@NotNull Class<? extends Exception> clazz,
			@NotNull FunctionEx<@NotNull Exception, @NotNull Exception> exceptionFn) {
		return mapException(e -> clazz.isAssignableFrom(e.getClass()), exceptionFn);
	}

	/**
	 * Returns a new {@code Promise} which is obtained by calling
	 * a provided supplier of a new promise. If {@code this} promise
	 * is completed exceptionally, a supplier will not be called.
	 *
	 * <p>
	 * A supplier may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param fn a supplier of a new promise which will be called
	 *           if {@code this} promise completes successfully
	 */
	<U> @NotNull Promise<U> then(@NotNull SupplierEx<Promise<? extends U>> fn);

	/**
	 * Returns a new {@code Promise} which is obtained by mapping
	 * a result of {@code this} promise to some other promise.
	 * If {@code this} promise is completed exceptionally, a mapping
	 * function will not be applied.
	 *
	 * <p>
	 * A mapping function may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param fn a function to map the result of this {@code Promise}
	 *           to a new promise
	 * @return new {@code Promise} which is the result of function
	 * applied to the result of {@code this} promise
	 * @see CompletionStage#thenCompose(Function)
	 */
	<U> @NotNull Promise<U> then(@NotNull FunctionEx<? super T, Promise<? extends U>> fn);

	/**
	 * Returns a new {@code Promise} which is obtained by mapping
	 * a result of {@code this} promise to some other promise using one of
	 * provided mapping functions.
	 * <p>
	 * If predicate returns {@code true}, the first mapping function is used,
	 * otherwise the second mapping function is used.
	 * <p>
	 * If {@code this} promise is completed exceptionally, no mapping
	 * function will be applied.
	 *
	 * <p>
	 * A mapping functions may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param predicate a predicate that tests the result of a promise
	 *                  to decide which one of the two mapping functions
	 *                  will be applied to the promise result
	 * @param fn        a function to map the result of this {@code Promise}
	 *                  to a new promise if a predicate returns {@code true}
	 * @param elseFn    a function to map the result of this {@code Promise}
	 *                  to a new promise if a predicate returns {@code false}
	 * @return new {@code Promise} which is the result of one of two
	 * mapping functions applied to the result of {@code this} promise
	 */
	default <U> @NotNull Promise<U> thenIfElse(@NotNull Predicate<? super T> predicate,
			@NotNull FunctionEx<? super T, Promise<? extends U>> fn, @NotNull FunctionEx<? super T, Promise<? extends U>> elseFn) {
		return then(t -> predicate.test(t) ? fn.apply(t) : elseFn.apply(t));
	}

	/**
	 * Returns a new {@code Promise} which is obtained by mapping
	 * a result of {@code this} promise to some other promise of the same result type.
	 * <p>
	 * If predicate returns {@code true}, the mapping function is used,
	 * otherwise the returned promise has the same result as {@code this} promise.
	 * <p>
	 * If {@code this} promise is completed exceptionally, a mapping
	 * function will not be applied.
	 *
	 * <p>
	 * A mapping function may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param predicate a predicate that tests the result of a promise
	 *                  to decide whether a mapping function
	 *                  will be applied to the promise result
	 * @param fn        a function to map the result of this {@code Promise}
	 *                  to a new promise if a predicate returns {@code true}
	 * @return new {@code Promise} which either has the original result or is a promise
	 * obtained by mapping the result of {@code this} promise to a new promise
	 */
	default @NotNull Promise<T> thenIf(@NotNull Predicate<? super T> predicate,
			@NotNull FunctionEx<? super T, Promise<? extends T>> fn) {
		return thenIfElse(predicate, fn, Promise::of);
	}

	/**
	 * Returns a new {@code Promise} which is obtained from a promise supplier
	 * in case a promise result is {@code null}
	 * <p>
	 * If a promise result is not {@code null}, the returned promise
	 * has the same result as {@code this} promise.
	 * <p>
	 * If {@code this} promise is completed exceptionally, a supplier will not be called.
	 *
	 * <p>
	 * A supplier may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param supplier a supplier of a new promise in case an original
	 *                 promise result is {@code null}
	 * @return new {@code Promise} where {@code null} result is replaced with a
	 * a new promise supplied by a given supplier
	 */
	default @NotNull Promise<T> thenIfNull(@NotNull SupplierEx<Promise<? extends T>> supplier) {
		return thenIfElse(Objects::isNull, $ -> supplier.get(), Promise::of);
	}

	/**
	 * Returns a new {@code Promise} which is obtained by mapping
	 * a non-{@code null} result of {@code this} promise to some other promise.
	 * <p>
	 * If a promise result is {@code null}, the returned promise
	 * has the same {@code null} result as {@code this} promise.
	 * <p>
	 * If {@code this} promise is completed exceptionally, a mapping
	 * function will not be applied.
	 *
	 * <p>
	 * A mapping function may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param fn a function to map the result of this {@code Promise}
	 *           to a new promise if a result of {@code this} promise is {@code null}
	 * @return new {@code Promise} which is either obtained by mapping a {@code null}
	 * result of {@code this} promise or is a promise with a {@code null} result
	 */
	default <U> @NotNull Promise<U> thenIfNonNull(@NotNull FunctionEx<? super @NotNull T, Promise<? extends U>> fn) {
		return thenIfElse(Objects::nonNull, fn, $ -> Promise.of(null));
	}

	/**
	 * Returns a new {@code Promise} which is obtained by mapping
	 * a result and an exception of {@code this} promise to some other promise.
	 * If {@code this} promise is completed exceptionally, an exception
	 * passed to a mapping function is guaranteed to be not null.
	 *
	 * <p>
	 * A function may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param fn a function to map a result and
	 *           an exception of {@code this} promise to some other promise
	 * @return new {@code Promise} which is the result of function
	 * applied to the result and exception of {@code this} promise
	 */
	<U> @NotNull Promise<U> then(@NotNull BiFunctionEx<? super T, @Nullable Exception, Promise<? extends U>> fn);

	/**
	 * Returns a new {@code Promise} which is obtained by mapping
	 * either a result or an exception of {@code this} promise to some other promises.
	 * If {@code this} promise is completed successfully, the first function will be called mapping the
	 * result to a promise of some other value. Otherwise, second function will be called mapping the exception
	 * to a promise of some other value.
	 *
	 * <p>
	 * Each function may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param fn          a function to map a result of {@code this} promise to a dofferent promise
	 * @param exceptionFn a function to map an exception of {@code this} promise a different promise
	 * @return new {@code Promise} which is a result of either first or second
	 * function applied either to a result or an exception of {@code this} promise.
	 */
	default <U> @NotNull Promise<U> then(
			@NotNull FunctionEx<? super T, Promise<? extends U>> fn,
			@NotNull FunctionEx<@NotNull Exception, Promise<? extends U>> exceptionFn) {
		return then((v, e) -> e == null ? fn.apply(v) : exceptionFn.apply(e));
	}

	/**
	 * Subscribes given consumer to be executed
	 * after this {@code Promise} completes (either successfully or exceptionally) and
	 * promise result and exception satisfy a given {@link BiPredicate}.
	 * Returns a new {@code Promise}.
	 *
	 * <p>
	 * A consumer may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param predicate a predicate that tests a result and an exception of {@code this} promise
	 * @param fn        a consumer that consumes a result
	 *                  and an exception of {@code this} promise
	 */
	default @NotNull Promise<T> when(@NotNull BiPredicate<? super T, @Nullable Exception> predicate, @NotNull BiConsumerEx<? super T, Exception> fn) {
		return then((v, e) -> {
			if (predicate.test(v, e)) {
				fn.accept(v, e);
			}
			return Promise.of(v, e);
		});
	}

	/**
	 * Subscribes given consumers to be executed
	 * after {@code this} Promise completes (either successfully or exceptionally)
	 * and promise result and exception satisfy a given {@link BiPredicate}.
	 * The first consumer will be executed if {@code this} Promise completes
	 * successfully, otherwise the second promise will be executed.
	 * Returns a new {@code Promise}.
	 *
	 * <p>
	 * Each consumer may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param predicate   a predicate that tests a result and an exception of {@code this} promise
	 * @param fn          consumer that consumes a result of {@code this} promise
	 * @param exceptionFn consumer that consumes an exception of {@code this} promise
	 */
	default @NotNull Promise<T> when(@NotNull BiPredicate<? super T, @Nullable Exception> predicate,
			@Nullable ConsumerEx<? super T> fn,
			@Nullable ConsumerEx<@NotNull Exception> exceptionFn) {
		return when(predicate, (v, e) -> {
			if (e == null) {
				//noinspection ConstantConditions
				fn.accept(v);
			} else {
				//noinspection ConstantConditions
				exceptionFn.accept(e);
			}
		});
	}

	/**
	 * Subscribes given runnable to be executed
	 * after this {@code Promise} completes (either successfully or exceptionally)
	 * and promise result and exception satisfy a given {@link BiPredicate}.
	 * Returns a new {@code Promise}.
	 *
	 * <p>
	 * A runnable may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param predicate a predicate that tests a result and an exception of {@code this} promise
	 * @param action    runnable to be executed after {@code this} promise completes
	 *                  and promise result and exception satisfy a given {@link BiPredicate}
	 */
	default @NotNull Promise<T> when(@NotNull BiPredicate<? super T, @Nullable Exception> predicate, @NotNull RunnableEx action) {
		return when(predicate, (v, e) -> action.run());
	}

	/**
	 * Subscribes given consumer to be executed
	 * after this {@code Promise} completes (either successfully or exceptionally).
	 * Returns a new {@code Promise}.
	 *
	 * <p>
	 * A consumer may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param fn a consumer that consumes a result
	 *           and an exception of {@code this} promise
	 */
	default @NotNull Promise<T> whenComplete(@NotNull BiConsumerEx<? super T, Exception> fn) {
		return when(P.isComplete(), fn);
	}

	/**
	 * Subscribes given consumers to be executed
	 * after {@code this} Promise completes (either successfully or exceptionally).
	 * The first consumer will be executed if {@code this} Promise completes
	 * successfully, otherwise the second promise will be executed.
	 * Returns a new {@code Promise}.
	 *
	 * <p>
	 * Each consumer may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param fn          consumer that consumes a result of {@code this} promise
	 * @param exceptionFn consumer that consumes an exception of {@code this} promise
	 */
	default @NotNull Promise<T> whenComplete(ConsumerEx<? super T> fn, ConsumerEx<@NotNull Exception> exceptionFn) {
		return when(P.isComplete(), fn, exceptionFn);
	}

	/**
	 * Subscribes given runnable to be executed
	 * after this {@code Promise} completes (either successfully or exceptionally).
	 * Returns a new {@code Promise}.
	 *
	 * <p>
	 * A runnable may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param action runnable to be executed after {@code this} promise completes
	 */
	default @NotNull Promise<T> whenComplete(@NotNull RunnableEx action) {
		return when(P.isComplete(), action);
	}

	/**
	 * Subscribes given consumer to be executed
	 * after this {@code Promise} completes successfully.
	 * Returns a new {@code Promise}.
	 *
	 * <p>
	 * A consumer may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param fn consumer that consumes a result of {@code this} promise
	 */
	default @NotNull Promise<T> whenResult(ConsumerEx<? super T> fn) {
		return when(P.isResult(), fn, null);
	}

	/**
	 * Subscribes given consumer to be executed
	 * after this {@code Promise} completes successfully
	 * and a result of this {@code Promise} satisfy a given {@link Predicate}.
	 * Returns a new {@code Promise}.
	 *
	 * <p>
	 * A consumer may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param predicate a predicate that tests a result of {@code this} promise
	 * @param fn        consumer that consumes a result of {@code this} promise
	 */
	default @NotNull Promise<T> whenResult(@NotNull Predicate<? super T> predicate, ConsumerEx<? super T> fn) {
		return when(P.isResult(predicate), fn, null);
	}

	/**
	 * Subscribes given runnable to be executed
	 * after this {@code Promise} completes successfully
	 * and a result of this {@code Promise} satisfy a given {@link Predicate}.
	 * Returns a new {@code Promise}.
	 *
	 * <p>
	 * A runnable may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param action runnable to be executed after {@code this} promise
	 *               completes successfully
	 */
	default @NotNull Promise<T> whenResult(@NotNull RunnableEx action) {
		return when(P.isResult(), action);
	}

	/**
	 * Subscribes given runnable to be executed
	 * after this {@code Promise} completes successfully
	 * and a result of this {@code Promise} satisfy a given {@link Predicate}.
	 * Returns a new {@code Promise}.
	 *
	 * <p>
	 * A runnable may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param predicate a predicate that tests a result of {@code this} promise
	 * @param action    runnable to be executed after {@code this} promise
	 *                  completes successfully and a result of this {@code Promise}
	 *                  satisfy a given {@link Predicate}
	 */
	default @NotNull Promise<T> whenResult(@NotNull Predicate<? super T> predicate, @NotNull RunnableEx action) {
		return when(P.isResult(predicate), action);
	}

	/**
	 * Subscribes given consumer to be executed
	 * after this {@code Promise} completes exceptionally.
	 * Returns a new {@code Promise}.
	 *
	 * <p>
	 * A consumer may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param fn consumer that consumes an exception of {@code this} promise
	 */
	default @NotNull Promise<T> whenException(@NotNull ConsumerEx<@NotNull Exception> fn) {
		return when(P.isException(), null, fn);
	}

	/**
	 * Subscribes given consumer to be executed
	 * after this {@code Promise} completes exceptionally
	 * and an exception of this {@code Promise} satisfy a given {@link Predicate}.
	 * Returns a new {@code Promise}.
	 *
	 * <p>
	 * A consumer may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param predicate a predicate that tests an exception of {@code this} promise
	 * @param fn        consumer that consumes an exception of {@code this} promise
	 */
	default @NotNull Promise<T> whenException(@NotNull Predicate<Exception> predicate, @NotNull ConsumerEx<@NotNull Exception> fn) {
		return when(P.isException(predicate), null, fn);
	}

	/**
	 * Subscribes given consumer to be executed
	 * after this {@code Promise} completes exceptionally
	 * and an exception of this {@code Promise} is an instance of a given exception {@link Class}.
	 * Returns a new {@code Promise}.
	 *
	 * <p>
	 * A consumer may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param clazz an exception class that exception is tested against. A consumer is called
	 *              only if an exception of {@code this} promise is an instance of the specified class
	 * @param fn    consumer that consumes an exception of {@code this} promise
	 */
	default @NotNull Promise<T> whenException(@NotNull Class<? extends Exception> clazz, @NotNull ConsumerEx<@NotNull Exception> fn) {
		return when(P.isException(clazz), null, fn);
	}

	/**
	 * Subscribes given runnable to be executed
	 * after this {@code Promise} completes exceptionally.
	 * Returns a new {@code Promise}.
	 *
	 * <p>
	 * A runnable may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param action runnable to be executed after {@code this} promise
	 *               completes exceptionally
	 */
	default @NotNull Promise<T> whenException(@NotNull RunnableEx action) {
		return when(P.isException(), action);
	}

	/**
	 * Subscribes given runnable to be executed
	 * after this {@code Promise} completes exceptionally
	 * and an exception of this {@code Promise} satisfy a given {@link Predicate}.
	 * Returns a new {@code Promise}.
	 *
	 * <p>
	 * A runnable may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param predicate a predicate that tests an exception of {@code this} promise
	 * @param action    runnable to be executed after {@code this} promise
	 *                  completes exceptionally and an exception of this {@code Promise}
	 *                  satisfies a given {@link Predicate}.
	 */
	default @NotNull Promise<T> whenException(@NotNull Predicate<Exception> predicate, @NotNull RunnableEx action) {
		return when(P.isException(predicate), action);
	}

	/**
	 * Subscribes given runnable to be executed
	 * after this {@code Promise} completes exceptionally
	 * and an exception of this {@code Promise} is an instance of a given exception {@link Class}.
	 * Returns a new {@code Promise}.
	 *
	 * <p>
	 * A runnable may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param clazz  an exception class that exception is tested against. A consumer is called
	 *               only if an exception of {@code this} promise is an instance of the specified class
	 * @param action runnable to be executed after {@code this} promise
	 *               completes exceptionally and an exception of this {@code Promise}
	 *               is an instance of a given exception {@link Class}.
	 */
	default @NotNull Promise<T> whenException(@NotNull Class<? extends Exception> clazz, @NotNull RunnableEx action) {
		return when(P.isException(clazz), action);
	}

	/**
	 * Casts {@code this} promise to a promise of some other type {@link U}
	 * <p>
	 * Usage example:
	 * <pre>
	 * Promise&lt;Object&gt; promise = Promise.of(255);
	 * promise.&lt;Integer&gt;cast()
	 *      .map(Integer::toHexString)
	 *      .whenResult(hex -> System.out.println(hex));
	 * </pre>
	 *
	 * @param <U> a type to which a promise result will be cast
	 * @return a promise of type {@link U}
	 * @throws ClassCastException if a promise result is not {@code null}
	 *                            and a cast could not be made
	 */
	@SuppressWarnings("unchecked")
	default @NotNull <U> Promise<U> cast() {
		return (Promise<U>) this;
	}

	/**
	 * Casts {@code this} promise to a promise of some other type {@link U}
	 * <p>
	 * Usage example:
	 * <pre>
	 * Promise&lt;Object&gt; promise = Promise.of(255);
	 * promise.cast(Integer.class)
	 *      .map(Integer::toHexString)
	 *      .whenResult(hex -> System.out.println(hex));
	 * </pre>
	 *
	 * @param cls a class to which a result of {@code this} promise will be cast
	 * @param <U> a type to which a promise result will be cast
	 * @return a promise of type {@link U}
	 * @throws ClassCastException if a promise result is not {@code null}
	 *                            and a cast could not be made
	 */
	@SuppressWarnings("unchecked")
	default @NotNull <U> Promise<U> cast(Class<? extends U> cls) {
		return (Promise<U>) this;
	}

	/**
	 * Returns a new {@code Promise} that, when this and the other
	 * given {@code Promise} both complete, is executed with the two
	 * results as arguments to the supplied function.
	 *
	 * @param other the other {@code Promise}
	 * @param fn    the function to use to compute the value of
	 *              the returned {@code Promise}
	 * @return new {@code Promise}
	 */
	@Contract(pure = true)
	<U, V> @NotNull Promise<V> combine(@NotNull Promise<? extends U> other, @NotNull BiFunctionEx<? super T, ? super U, ? extends V> fn);

	/**
	 * Returns a new {@code Promise} when both
	 * this and provided {@code other}
	 * {@code Promises} complete.
	 *
	 * @param other the other {@code Promise}
	 * @return {@code Promise} of {@code null}
	 * when both this and other
	 * {@code Promise} complete
	 */
	@Contract(pure = true)
	@NotNull Promise<Void> both(@NotNull Promise<?> other);

	/**
	 * Returns the {@code Promise} which was completed first.
	 *
	 * @param other the other {@code Promise}
	 * @return the first completed {@code Promise}
	 */
	@Contract(pure = true)
	@NotNull Promise<T> either(@NotNull Promise<? extends T> other);

	/**
	 * Returns {@code Promise} that always completes successfully
	 * with result or exception wrapped in {@link Try}.
	 */
	@Contract(pure = true)
	@NotNull Promise<Try<T>> toTry();

	/**
	 * Waits for result and discards it.
	 */
	@Contract(pure = true)
	@NotNull Promise<Void> toVoid();

	/**
	 * Calls a {@code callback} after {@code this} promise is completed, passing
	 * both a result and an exception of a promise to the callback. This method is
	 * similar to {@link #whenComplete(BiConsumerEx)} but does not create or return
	 * a new promise. A {@link Callback} interface also prohibits throwing checked exceptions.
	 * <p>
	 * In most cases {@link #whenComplete(BiConsumerEx)} is preferred. Fall back to using this method
	 * only if you expect a performance hit from constantly calling {@link #whenComplete(BiConsumerEx)}
	 * and creating new promises as a result.
	 *
	 * @param callback a callback to be called once a promise completes
	 */
	@Override
	void run(@NotNull Callback<? super T> callback);

	/**
	 * Wraps {@code Promise} into {@link CompletableFuture}.
	 */
	@Contract(pure = true)
	@NotNull CompletableFuture<T> toCompletableFuture();

}
