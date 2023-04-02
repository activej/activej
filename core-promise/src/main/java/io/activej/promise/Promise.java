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
import io.activej.async.function.*;
import io.activej.common.collection.Try;
import io.activej.common.function.*;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.exception.FatalErrorHandler.getExceptionOrThrowError;
import static io.activej.common.exception.FatalErrorHandler.handleError;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static io.activej.reactor.util.RunnableWithContext.runnableOf;

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
	static Promise<Void> complete() {
		return (Promise<Void>) CompleteNullPromise.INSTANCE;
	}

	/**
	 * Creates successfully completed {@code Promise}.
	 *
	 * @param value result of Promise. If value is {@code null},
	 *              returns {@link CompleteNullPromise}, otherwise
	 *              {@link CompleteResultPromise}
	 */
	static <T> Promise<T> of(@Nullable T value) {
		return value != null ? new CompleteResultPromise<>(value) : CompleteNullPromise.instance();
	}

	/**
	 * Creates an exceptionally completed {@code Promise}.
	 *
	 * @param e Exception
	 */
	static <T> Promise<T> ofException(Exception e) {
		return new CompleteExceptionallyPromise<>(e);
	}

	/**
	 * Creates and returns a new {@link SettablePromise}
	 * that is accepted by the provided {@link ConsumerEx} of
	 * {@link SettablePromise}
	 */
	static <T> Promise<T> ofCallback(CallbackSupplierEx<T> fn) {
		SettablePromise<T> cb = new SettablePromise<>();
		try {
			fn.get(cb);
		} catch (Exception ex) {
			handleError(ex, fn);
			return Promise.ofException(ex);
		}
		return cb;
	}

	static <T, R> Promise<R> ofCallback(T value, CallbackFunctionEx<? super T, R> fn) {
		SettablePromise<R> cb = new SettablePromise<>();
		try {
			fn.apply(value, cb);
		} catch (Exception ex) {
			handleError(ex, fn);
			return Promise.ofException(ex);
		}
		return cb;
	}

	static <T, R> Promise<R> ofCallback(T value, @Nullable Exception exception, CallbackBiFunctionEx<? super T, @Nullable Exception, R> fn) {
		SettablePromise<R> cb = new SettablePromise<>();
		try {
			fn.apply(value, exception, cb);
		} catch (Exception ex) {
			handleError(ex, fn);
			return Promise.ofException(ex);
		}
		return cb;
	}

	static <T, R> Promise<R> ofCallback(T value, @Nullable Exception exception,
			CallbackFunctionEx<? super T, R> fn,
			CallbackFunctionEx<Exception, R> fnException) {
		SettablePromise<R> cb = new SettablePromise<>();
		try {
			if (exception == null) fn.apply(value, cb);
			else fnException.apply(exception, cb);
		} catch (Exception ex) {
			handleError(ex, fn);
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
	static <T> Promise<T> ofOptional(Optional<T> optional) {
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
	static <T> Promise<T> ofOptional(Optional<T> optional, Supplier<? extends Exception> errorSupplier) {
		if (optional.isPresent()) return Promise.of(optional.get());
		return Promise.ofException(errorSupplier.get());
	}

	/**
	 * Creates a completed {@code Promise} from {@code T value} and
	 * {@code Exception e} parameters, any of them can be {@code null}.
	 * Useful for {@link #then} passthroughs
	 * (for example, when mapping specific exceptions).
	 *
	 * @param value value to wrap when exception is null
	 * @param e     possibly-null exception, determines type of promise completion
	 */
	static <T> Promise<T> of(@Nullable T value, @Nullable Exception e) {
		checkArgument(!(value != null && e != null), "Either value or exception should be 'null'");
		return e == null ? of(value) : ofException(e);
	}

	/**
	 * Returns a new {@link CompletePromise} or {@link CompleteExceptionallyPromise}
	 * based on the provided {@link Try}.
	 */
	static <T> Promise<T> ofTry(Try<T> t) {
		return t.reduce(Promise::of, Promise::ofException);
	}

	/**
	 * Creates a {@code Promise} wrapper around default
	 * Java {@code CompletableFuture} and runs it immediately.
	 *
	 * @return a new {@code Promise} with a result of the given future
	 */
	static <T> Promise<T> ofFuture(CompletableFuture<? extends T> future) {
		return ofCompletionStage(future);
	}

	/**
	 * Wraps Java {@link CompletionStage} in a {@code Promise}, running it in current reactor.
	 *
	 * @param completionStage completion stage itself
	 * @return result of the given completionStage wrapped in a {@code Promise}
	 */
	static <T> Promise<T> ofCompletionStage(CompletionStage<? extends T> completionStage) {
		return ofCallback(cb -> {
			Reactor reactor = getCurrentReactor();
			reactor.startExternalTask();
			completionStage.whenCompleteAsync((result, throwable) -> {
				reactor.execute(runnableOf(cb, () -> {
					if (throwable == null) {
						cb.set(result, null);
					} else {
						Exception e = getExceptionOrThrowError(throwable);
						handleError(e, cb);
						cb.set(null, e);
					}
				}));
				reactor.completeExternalTask();
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
	static <T> Promise<T> ofFuture(Executor executor, Future<? extends T> future) {
		return ofCallback(cb -> {
			Reactor reactor = Reactor.getCurrentReactor();
			reactor.startExternalTask();
			try {
				executor.execute(() -> {
					try {
						T value = future.get();
						reactor.execute(runnableOf(cb, () -> cb.set(value)));
					} catch (ExecutionException ex) {
						reactor.execute(runnableOf(cb, () -> {
							Exception e = getExceptionOrThrowError(ex.getCause());
							handleError(e, cb);
							cb.setException(e);
						}));
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						reactor.execute(runnableOf(cb, () -> cb.setException(e)));
					} catch (CancellationException e) {
						reactor.execute(runnableOf(cb, () -> cb.setException(e)));
					} catch (Throwable throwable) {
						reactor.execute(runnableOf(cb, () -> {
							Exception e = getExceptionOrThrowError(throwable);
							handleError(e, cb);
							cb.setException(e);
						}));
					} finally {
						reactor.completeExternalTask();
					}
				});
			} catch (RejectedExecutionException e) {
				reactor.completeExternalTask();
				cb.setException(e);
			}
		});
	}

	/**
	 * Runs some task in another thread (executed by a given {@code Executor})
	 * and returns a {@code Promise} for it. Also manages external task count
	 * for current reactor, so it won't shut down until the task is complete.
	 *
	 * @param executor executor to execute the task concurrently
	 * @param supplier the task itself
	 * @return {@code Promise} for the given task
	 */
	static <T> Promise<T> ofBlocking(Executor executor, SupplierEx<? extends T> supplier) {
		return ofCallback(cb -> {
			Reactor reactor = Reactor.getCurrentReactor();
			reactor.startExternalTask();
			try {
				executor.execute(() -> {
					try {
						T result = supplier.get();
						reactor.execute(runnableOf(cb, () -> cb.set(result)));
					} catch (Throwable throwable) {
						reactor.execute(runnableOf(cb, () -> {
							Exception e = getExceptionOrThrowError(throwable);
							handleError(e, cb);
							cb.setException(e);
						}));
					} finally {
						reactor.completeExternalTask();
					}
				});
			} catch (RejectedExecutionException e) {
				reactor.completeExternalTask();
				cb.setException(e);
			}
		});
	}

	/**
	 * Same as {@link #ofBlocking(Executor, SupplierEx)}, but without a result
	 * (returned {@code Promise} is only a marker of completion).
	 */
	static Promise<Void> ofBlocking(Executor executor, RunnableEx runnable) {
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
	 * completion will be posted to the next reactor tick.
	 * Otherwise, does nothing.
	 */
	@Contract(pure = true)
	Promise<T> async();

	Promise<T> subscribe(Callback<? super T> cb);

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
	default <U> Promise<U> map(FunctionEx<? super T, ? extends U> fn) {
		return thenCallback((t, cb) -> cb.set(fn.apply(t)));
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
	default <U> Promise<U> map(BiFunctionEx<? super T, @Nullable Exception, ? extends U> fn) {
		return thenCallback((t, e, cb) -> cb.set(fn.apply(t, e)));
	}

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
	default <U> Promise<U> map(FunctionEx<? super T, ? extends U> fn, FunctionEx<Exception, ? extends U> exceptionFn) {
		return thenCallback((t, e, cb) -> cb.set(e == null ? fn.apply(t) : exceptionFn.apply(e)));
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
	default Promise<T> mapException(FunctionEx<Exception, Exception> exceptionFn) {
		return thenCallback((t, e, cb) -> {
			if (e == null) cb.set(t);
			else cb.setException(exceptionFn.apply(e));
		});
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
	default <E extends Exception> Promise<T> mapException(Class<E> clazz,
			FunctionEx<? super E, ? extends Exception> exceptionFn) {
		return thenCallback((t, e, cb) -> {
			if (e == null) cb.set(t);
			else cb.setException(clazz.isAssignableFrom(e.getClass()) ? exceptionFn.apply((E) e) : e);
		});
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
	default <U> Promise<U> then(AsyncSupplierEx<U> fn) {
		return thenCallback(cb -> fn.get().subscribe(cb));
	}

	default <U> Promise<U> thenCallback(CallbackSupplierEx<U> fn) {
		return thenCallback((t, e, cb) -> {
			if (e == null) fn.get(cb);
			else cb.setException(e);
		});
	}

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
	default <U> Promise<U> then(AsyncFunctionEx<? super T, U> fn) {
		return thenCallback((t, cb) -> fn.apply(t).subscribe(cb));
	}

	default <U> Promise<U> thenCallback(CallbackFunctionEx<? super T, U> fn) {
		return thenCallback((t, e, cb) -> {
			if (e == null) fn.apply(t, cb);
			else cb.setException(e);
		});
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
	default <U> Promise<U> then(AsyncBiFunctionEx<? super T, @Nullable Exception, U> fn) {
		return thenCallback((t, e, cb) -> fn.apply(t, e).subscribe(cb));
	}

	<U> Promise<U> thenCallback(CallbackBiFunctionEx<? super T, @Nullable Exception, U> fn);

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
	default <U> Promise<U> then(
			AsyncFunctionEx<? super T, U> fn,
			AsyncFunctionEx<Exception, U> exceptionFn) {
		return thenCallback((t, e, cb) -> (e == null ? fn.apply(t) : exceptionFn.apply(e)).subscribe(cb));
	}

	default <U> Promise<U> thenCallback(
			CallbackFunctionEx<? super T, U> fn,
			CallbackFunctionEx<Exception, U> exceptionFn) {
		return thenCallback((t, e, cb) -> {
			if (e == null) fn.apply(t, cb);
			else exceptionFn.apply(e, cb);
		});
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
	default Promise<T> whenComplete(BiConsumerEx<? super T, Exception> fn) {
		return thenCallback((t, e, cb) -> {
			fn.accept(t, e);
			cb.set(t, e);
		});
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
	default Promise<T> whenComplete(ConsumerEx<? super T> fn, ConsumerEx<Exception> exceptionFn) {
		return thenCallback((t, e, cb) -> {
			fn.accept(t);
			cb.set(t, e);
		});
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
	default Promise<T> whenComplete(RunnableEx action) {
		return thenCallback((t, e, cb) -> {
			action.run();
			cb.set(t, e);
		});
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
	default Promise<T> whenResult(ConsumerEx<? super T> fn) {
		return thenCallback((t, cb) -> {
			fn.accept(t);
			cb.set(t);
		});
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
	default Promise<T> whenResult(RunnableEx action) {
		return thenCallback((t, cb) -> {
			action.run();
			cb.set(t);
		});
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
	default Promise<T> whenException(ConsumerEx<Exception> fn) {
		return thenCallback((t, e, cb) -> {
			if (e != null) fn.accept(e);
			cb.set(t, e);
		});
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
	default <E extends Exception> Promise<T> whenException(Class<E> clazz, ConsumerEx<? super E> fn) {
		return thenCallback((t, e, cb) -> {
			if (e != null && clazz.isAssignableFrom(e.getClass())) fn.accept((E) e);
			cb.set(t, e);
		});
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
	default Promise<T> whenException(RunnableEx action) {
		return thenCallback((t, e, cb) -> {
			if (e != null) action.run();
			cb.set(t, e);
		});
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
	default Promise<T> whenException(Class<? extends Exception> clazz, RunnableEx action) {
		return thenCallback((t, e, cb) -> {
			if (e != null && clazz.isAssignableFrom(e.getClass())) action.run();
			cb.set(t, e);
		});
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
	default <U> Promise<U> cast() {
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
	default <U> Promise<U> cast(Class<? extends U> cls) {
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
	<U, V> Promise<V> combine(Promise<? extends U> other, BiFunctionEx<? super T, ? super U, ? extends V> fn);

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
	Promise<Void> both(Promise<?> other);

	/**
	 * Returns the {@code Promise} which was completed first.
	 *
	 * @param other the other {@code Promise}
	 * @return the first completed {@code Promise}
	 */
	@Contract(pure = true)
	Promise<T> either(Promise<? extends T> other);

	/**
	 * Returns {@code Promise} that always completes successfully
	 * with result or exception wrapped in {@link Try}.
	 */
	@Contract(pure = true)
	Promise<Try<T>> toTry();

	/**
	 * Waits for result and discards it.
	 */
	@Contract(pure = true)
	Promise<Void> toVoid();

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
	 * @param cb a callback to be called once a promise completes
	 */
	@ApiStatus.Internal
	void next(NextPromise<? super T, ?> cb);

	@ApiStatus.Internal
	@Override
	default void call(Callback<? super T> cb) {
		subscribe(cb);
	}

	/**
	 * Wraps {@code Promise} into {@link CompletableFuture}.
	 */
	@Contract(pure = true)
	CompletableFuture<T> toCompletableFuture();

}
