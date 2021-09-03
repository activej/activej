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
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.activej.common.Checks.checkArgument;
import static io.activej.eventloop.util.RunnableWithContext.wrapContext;

/**
 * Replacement of default Java {@link CompletionStage} interface with
 * optimized design, which allows to handle different scenarios more
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
	@NotNull
	@SuppressWarnings("unchecked")
	static CompleteNullPromise<Void> complete() {
		return (CompleteNullPromise<Void>) CompleteNullPromise.INSTANCE;
	}

	/**
	 * Creates successfully completed {@code Promise}.
	 *
	 * @param value result of Promise. If value is {@code null},
	 *              returns {@link CompleteNullPromise}, otherwise
	 *              {@link CompleteResultPromise}
	 */
	@NotNull
	static <T> CompletePromise<T> of(@Nullable T value) {
		return value != null ? new CompleteResultPromise<>(value) : CompleteNullPromise.instance();
	}

	/**
	 * Creates an exceptionally completed {@code Promise}.
	 *
	 * @param e Exception
	 */
	@NotNull
	static <T> CompleteExceptionallyPromise<T> ofException(@NotNull Exception e) {
		return new CompleteExceptionallyPromise<>(e);
	}

	/**
	 * Creates and returns a new {@link SettablePromise}
	 * that is accepted by the provided {@link Consumer} of
	 * {@link SettablePromise}
	 */
	@NotNull
	static <T> Promise<T> ofCallback(@NotNull Consumer<@NotNull SettablePromise<T>> callbackConsumer) {
		SettablePromise<T> cb = new SettablePromise<>();
		callbackConsumer.accept(cb);
		return cb;
	}

	/**
	 * @see #ofOptional(Optional, Supplier)
	 */
	@NotNull
	@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
	static <T> Promise<T> ofOptional(@NotNull Optional<T> optional) {
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
	@NotNull
	@SuppressWarnings({"OptionalUsedAsFieldOrParameterType"})
	static <T> Promise<T> ofOptional(@NotNull Optional<T> optional, @NotNull Supplier<? extends Exception> errorSupplier) {
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
	@NotNull
	static <T> Promise<T> of(@Nullable T value, @Nullable Exception e) {
		checkArgument(!(value != null && e != null), "Either value or exception should be 'null'");
		return e == null ? of(value) : ofException(e);
	}

	/**
	 * Returns a new {@link CompletePromise} or {@link CompleteExceptionallyPromise}
	 * based on the provided {@link Try}.
	 */
	@NotNull
	static <T> Promise<T> ofTry(@NotNull Try<T> t) {
		return t.reduce(Promise::of, Promise::ofException);
	}

	/**
	 * Creates a {@code Promise} wrapper around default
	 * Java {@code CompletableFuture} and runs it immediately.
	 *
	 * @return a new {@code Promise} with a result of the given future
	 */
	@NotNull
	static <T> Promise<T> ofFuture(@NotNull CompletableFuture<? extends T> future) {
		return ofCompletionStage(future);
	}

	/**
	 * Wraps Java {@link CompletionStage} in a {@code Promise}, running it in current eventloop.
	 *
	 * @param completionStage completion stage itself
	 * @return result of the given completionStage wrapped in a {@code Promise}
	 */
	@NotNull
	static <T> Promise<T> ofCompletionStage(CompletionStage<? extends T> completionStage) {
		return ofCallback(cb -> {
			Eventloop eventloop = Eventloop.getCurrentEventloop();
			eventloop.startExternalTask();
			completionStage.whenCompleteAsync((result, e) -> {
				if (!(e instanceof Exception)) throw (Error) e;
				eventloop.execute(wrapContext(cb, () -> cb.accept(result, (Exception) e)));
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
	@NotNull
	static <T> Promise<T> ofFuture(@NotNull Executor executor, @NotNull Future<? extends T> future) {
		return ofCallback(cb -> {
			Eventloop eventloop = Eventloop.getCurrentEventloop();
			eventloop.startExternalTask();
			try {
				executor.execute(() -> {
					try {
						T value = future.get();
						eventloop.execute(wrapContext(cb, () -> cb.set(value)));
					} catch (ExecutionException e) {
						eventloop.execute(wrapContext(cb, () -> cb.setException((Exception) e.getCause())));
					} catch (InterruptedException e) {
						eventloop.execute(wrapContext(cb, () -> cb.setException(e)));
					} catch (Exception e) {
						eventloop.execute(() -> eventloop.recordFatalError(e, future));
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
					} catch (RuntimeException e) {
						eventloop.execute(() -> eventloop.recordFatalError(e, supplier));
					} catch (Exception e) {
						eventloop.execute(wrapContext(cb, () -> cb.setException(e)));
					} catch (Throwable e) {
						eventloop.execute(() -> eventloop.recordFatalError(e, supplier));
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
	@NotNull
	static Promise<Void> ofBlocking(@NotNull Executor executor, @NotNull RunnableEx runnable) {
		return ofCallback(cb -> {
			Eventloop eventloop = Eventloop.getCurrentEventloop();
			eventloop.startExternalTask();
			try {
				executor.execute(() -> {
					try {
						runnable.run();
						eventloop.execute(wrapContext(cb, () -> cb.set(null)));
					} catch (RuntimeException e) {
						eventloop.execute(() -> eventloop.recordFatalError(e, runnable));
					} catch (Exception e) {
						eventloop.execute(wrapContext(cb, () -> cb.setException(e)));
					} catch (Throwable e) {
						eventloop.execute(() -> eventloop.recordFatalError(e, runnable));
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
	 * completion will be posted to next eventloop tick.
	 * Otherwise, does nothing.
	 */
	@Contract(pure = true)
	@NotNull Promise<T> async();

	@Contract(pure = true)
	@NotNull
	default Promise<T> post() {
		SettablePromise<T> result = new SettablePromise<>();
		this.run(result::post);
		return result;
	}

	/**
	 * Executes given {@code promise} after execution
	 * of this {@code Promise} completes.
	 *
	 * @param promise given promise
	 * @param <U>     type of result
	 * @return subscribed {@code Promise}
	 */
	@Contract("_ -> param1")
	@NotNull <U, P extends Callback<? super T> & Promise<U>> Promise<U> next(@NotNull P promise);

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
	@NotNull <U> Promise<U> map(@NotNull FunctionEx<? super T, ? extends U> fn);

	/**
	 * Returns a new {@code Promise} which is obtained by mapping
	 * a result and an exception of {@code this} promise to some other value.
	 * If {@code this} promise is completed exceptionally, an exception
	 * passed to a mapping bi function is guaranteed to be not null.
	 *
	 * <p>
	 * A bi function may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param fn a bi function to map a result and
	 *           an exception of {@code this} promise to some other value
	 * @return new {@code Promise} whose result is the result of bi function
	 * applied to the result and exception of {@code this} promise
	 */
	@NotNull <U> Promise<U> map(@NotNull BiFunctionEx<? super T, @Nullable Exception, ? extends U> fn);

	default @NotNull <U> Promise<U> map(@NotNull FunctionEx<? super T, ? extends U> fn, @NotNull FunctionEx<@NotNull Exception, ? extends U> exceptionFn) {
		return map((v, e) -> e == null ? fn.apply(v) : exceptionFn.apply(e));
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
	 *           if {@code this} promise} completes successfully
	 */
	@NotNull <U> Promise<U> then(@NotNull SupplierEx<? extends Promise<? extends U>> fn);

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
	@NotNull <U> Promise<U> then(@NotNull FunctionEx<? super T, ? extends Promise<? extends U>> fn);

	/**
	 * Returns a new {@code Promise} which is obtained by mapping
	 * a result and an exception of {@code this} promise to some other promise.
	 * If {@code this} promise is completed exceptionally, an exception
	 * passed to a mapping bi function is guaranteed to be not null.
	 *
	 * <p>
	 * A bi function may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param fn a bi function to map a result and
	 *           an exception of {@code this} promise to some other promise
	 * @return new {@code Promise} which is the result of bi function
	 * applied to the result and exception of {@code this} promise
	 */
	@NotNull <U> Promise<U> then(@NotNull BiFunctionEx<? super T, @Nullable Exception, ? extends Promise<? extends U>> fn);

	/**
	 * Subscribes given bi consumer to be executed
	 * after this {@code Promise} completes (either successfully or exceptionally).
	 * Returns a new {@code Promise}.
	 *
	 * <p>
	 * A bi consumer may throw a checked exception. In this case
	 * the resulting promise is completed exceptionally with a
	 * thrown exception.
	 *
	 * @param action bi consumer that consumes a result
	 *               and an exception of {@code this} promise
	 */
	@NotNull
	default Promise<T> whenComplete(@NotNull BiConsumerEx<? super T, Exception> action) {
		return then((v, e) -> {
			action.accept(v, e);
			return Promise.of(v, e);
		});
	}

	/**
	 * Subscribes given runable to be executed
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
	@NotNull
	default Promise<T> whenComplete(@NotNull RunnableEx action) {
		return then((v, e) -> {
			action.run();
			return Promise.of(v, e);
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
	 * @param action consumer that consumes a result of {@code this} promise
	 */
	@NotNull
	default Promise<T> whenResult(ConsumerEx<? super T> action) {
		return then(v -> {
			action.accept(v);
			return Promise.of(v);
		});
	}

	/**
	 * Subscribes given runnable to be executed
	 * after this {@code Promise} completes successfully.
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
	@NotNull
	default Promise<T> whenResult(@NotNull RunnableEx action) {
		return then(v -> {
			action.run();
			return Promise.of(v);
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
	 * @param action consumer that consumes an exception of {@code this} promise
	 */
	@NotNull
	default Promise<T> whenException(@NotNull ConsumerEx<@NotNull Exception> action) {
		return then((v, e) -> {
			if (e != null) {
				action.accept(e);
			}
			return Promise.of(v, e);
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
	@NotNull
	default Promise<T> whenException(@NotNull RunnableEx action) {
		return then((v, e) -> {
			if (e != null) {
				action.run();
			}
			return Promise.of(v, e);
		});
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
	@NotNull <U, V>
	Promise<V> combine(@NotNull Promise<? extends U> other, @NotNull BiFunction<? super T, ? super U, ? extends V> fn);

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
	@NotNull
	Promise<Void> both(@NotNull Promise<?> other);

	/**
	 * Returns the {@code Promise} which was completed first.
	 *
	 * @param other the other {@code Promise}
	 * @return the first completed {@code Promise}
	 */
	@Contract(pure = true)
	@NotNull
	Promise<T> either(@NotNull Promise<? extends T> other);

	/**
	 * Returns {@code Promise} that always completes successfully
	 * with result or exception wrapped in {@link Try}.
	 */
	@Contract(pure = true)
	@NotNull
	Promise<Try<T>> toTry();

	/**
	 * Waits for result and discards it.
	 */
	@Contract(pure = true)
	@NotNull
	Promise<Void> toVoid();

	@Override
	void run(@NotNull Callback<? super T> action);

	/**
	 * Wraps {@code Promise} into {@link CompletableFuture}.
	 */
	@Contract(pure = true)
	@NotNull
	CompletableFuture<T> toCompletableFuture();

}
