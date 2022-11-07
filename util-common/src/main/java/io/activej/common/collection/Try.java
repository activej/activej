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

package io.activej.common.collection;

import io.activej.common.function.FunctionEx;
import io.activej.common.function.RunnableEx;
import io.activej.common.function.SupplierEx;
import io.activej.common.recycle.Recyclers;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.*;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static io.activej.common.exception.FatalErrorHandlers.handleError;

/**
 * A compound type that represents either a result or an exception
 * <p>
 * Semantically close to {@code Either<T, Exception>}
 *
 * @param <T> type of result
 * @see Either
 */
public final class Try<T> {
	static {
		Recyclers.register(Try.class, aTry -> Recyclers.recycle(aTry.result));
	}

	private final T result;

	private final @Nullable Exception exception;

	private Try(@Nullable T result, @Nullable Exception e) {
		this.result = result;
		this.exception = e;
	}

	/**
	 * Creates a new successful {@link Try} with a result
	 *
	 * @param result a result value of a {@link Try}
	 * @param <T>    a type of result
	 * @return a new instance of a successful {@link Try}
	 */
	public static <T> Try<T> of(@Nullable T result) {
		return new Try<>(result, null);
	}

	/**
	 * Creates a new {@link Try} which is either successful
	 * and has a result or is failed and has an exception
	 *
	 * @param result a result value of a {@link Try}
	 * @param e      an exception of a {@link Try}
	 * @param <T>    a type of result
	 * @return a new instance of a {@link Try} that is either successful or failed
	 */
	public static <T> Try<T> of(@Nullable T result, @Nullable Exception e) {
		checkArgument(result == null || e == null, "Either result or exception should be null");
		return new Try<>(result, e);
	}

	/**
	 * Creates a new failed {@link Try} with a given exception
	 *
	 * @param e   an exception of a {@link Try}
	 * @param <T> a type of result
	 * @return a new instance of a failed {@link Try}
	 */
	public static <T> Try<T> ofException(@NotNull Exception e) {
		return new Try<>(null, e);
	}

	/**
	 * Creates a new {@link Try} which is either successful or failed
	 * based on the result of a supplier call
	 * <p>
	 * If supplier throws exception a {@link Try} is failed.
	 * Otherwise, it will have supplied value as a result value of a {@link Try}
	 *
	 * @param computation a throwing supplier of a result of a {@link Try}
	 * @param <T>         a type of result
	 * @return a new instance of a {@link Try} that is either successful or failed
	 */
	public static <T> Try<T> wrap(@NotNull SupplierEx<T> computation) {
		try {
			return new Try<>(computation.get(), null);
		} catch (Exception ex) {
			handleError(ex, computation);
			return new Try<>(null, ex);
		}
	}

	/**
	 * Creates a new {@link Try} which is either successful or failed
	 * based on the result of a runnable call
	 * <p>
	 * If runnable throws exception a {@link Try} is failed.
	 * Otherwise, it will have {@code null} as a result value of a {@link Try}
	 *
	 * @param computation a throwing runnable
	 * @param <T>         a type of result
	 * @return a new instance of a {@link Try} that is either successful or failed
	 */
	public static <T> Try<T> wrap(@NotNull RunnableEx computation) {
		try {
			computation.run();
			return new Try<>(null, null);
		} catch (Exception ex) {
			handleError(ex, computation);
			return new Try<>(null, ex);
		}
	}

	/**
	 * Creates a new {@link Try} which is either successful or failed
	 * based on the result of a callable call
	 * <p>
	 * If callable throws a runtime exception a {@link Try} is failed.
	 * Otherwise, it will have supplied value as a result value of a {@link Try}
	 *
	 * @param computation a callable of a result of a {@link Try}
	 * @param <T>         a type of result
	 * @return a new instance of a {@link Try} that is either successful or failed
	 */
	public static <T> Try<T> wrap(@NotNull Callable<? extends T> computation) {
		try {
			@Nullable T result = computation.call();
			return new Try<>(result, null);
		} catch (Exception e) {
			handleError(e, computation);
			return new Try<>(null, e);
		}
	}

	/**
	 * Returns whether this {@link Try} is successful
	 */
	@Contract(pure = true)
	public boolean isSuccess() {
		return exception == null;
	}

	/**
	 * Returns whether this {@link Try} is failed
	 */
	@Contract(pure = true)
	public boolean isException() {
		return exception != null;
	}

	/**
	 * Returns a result of this {@link Try}, possibly {@code null}
	 */
	@Contract(pure = true)
	public T get() {
		return result;
	}

	/**
	 * Returns a result of this {@link Try} if a {@link Try} is successful.
	 * Otherwise, returns a given default value
	 *
	 * @param defaultValue a default value to be returned if this {@link Try} is failed
	 * @return a result of this {@link Try} or a given default value
	 */
	@Contract(pure = true)
	public T getElse(@Nullable T defaultValue) {
		return exception == null ? result : defaultValue;
	}

	/**
	 * Returns a result of this {@link Try} if a {@link Try} is successful.
	 * Otherwise, returns a supplied default value
	 *
	 * @param defaultValueSupplier a supplier of a default value to be returned if this {@link Try} is failed
	 * @return a result of this {@link Try} or a supplied default value
	 */
	@Contract(pure = true)
	public T getElseGet(@NotNull Supplier<? extends T> defaultValueSupplier) {
		return exception == null ? result : defaultValueSupplier.get();
	}

	/**
	 * Returns an exception of this {@link Try}, possibly {@code null}
	 */
	@Contract(pure = true)
	public @Nullable Exception getException() {
		return exception;
	}

	/**
	 * Consumes a result of this {@link Try} if it is successful.
	 * Otherwise, does nothing
	 * <p>
	 * Always returns this {@link Try}
	 *
	 * @param resultConsumer a consumer of a result
	 * @return this {@link Try}
	 */
	public @NotNull Try<T> ifSuccess(@NotNull Consumer<? super T> resultConsumer) {
		if (isSuccess()) {
			resultConsumer.accept(result);
		}
		return this;
	}

	/**
	 * Consumes an exception of this {@link Try} if it is failed.
	 * Otherwise, does nothing
	 * <p>
	 * Always returns this {@link Try}
	 *
	 * @param exceptionConsumer a consumer of an exception
	 * @return this {@link Try}
	 */
	public @NotNull Try<T> ifException(@NotNull Consumer<Exception> exceptionConsumer) {
		if (isException()) {
			exceptionConsumer.accept(exception);
		}
		return this;
	}

	/**
	 * Consumes both a result and an exception of this {@link Try}.
	 * <p>
	 * Always returns this {@link Try}
	 *
	 * @param consumer a consumer of a result and an exception
	 * @return this {@link Try}
	 */
	public @NotNull Try<T> consume(@NotNull BiConsumer<? super T, Exception> consumer) {
		consumer.accept(result, exception);
		return this;
	}

	/**
	 * Consumes a result of this {@link Try} if it is successful.
	 * Otherwise, consumes an exception
	 * <p>
	 * Always returns this {@link Try}
	 *
	 * @param resultConsumer    a consumer of a result
	 * @param exceptionConsumer a consumer of an exception
	 * @return this {@link Try}
	 */
	public @NotNull Try<T> consume(@NotNull Consumer<? super T> resultConsumer, @NotNull Consumer<Exception> exceptionConsumer) {
		if (isSuccess()) {
			resultConsumer.accept(result);
		} else {
			exceptionConsumer.accept(exception);
		}
		return this;
	}

	/**
	 * Transforms a failed {@link Try<T>} into a failed {@link Try<U>}
	 *
	 * @param <U> a result type of new failed {@link Try}
	 * @return a molded {@link Try}
	 */
	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	private <U> @NotNull Try<U> mold() {
		checkState(isException());
		return (Try<U>) this;
	}

	/**
	 * Applies a function to either this {@link Try}'s result or an exception
	 *
	 * @param function          a function to map a result value
	 * @param exceptionFunction a function to map an exception
	 * @param <U>               a type of mapping result
	 * @return a result of mapping of either a result or an exception of a {@link Try}
	 */
	@Contract(pure = true)
	public <U> U reduce(@NotNull Function<? super T, ? extends U> function, @NotNull Function<@NotNull Exception, ? extends U> exceptionFunction) {
		return exception == null ? function.apply(result) : exceptionFunction.apply(exception);
	}

	/**
	 * Applies a function to this {@link Try}'s result and an exception
	 *
	 * @param fn  a function to map a result value and an exception
	 * @param <U> a type of mapping result
	 * @return a result of mapping of a result and an exception of a {@link Try}
	 */
	@Contract(pure = true)
	public <U> U reduce(@NotNull BiFunction<? super T, Exception, ? extends U> fn) {
		return fn.apply(result, exception);
	}

	/**
	 * Returns a mapped {@link Try} obtained by mapping a result
	 * of this {@link Try} to a new value
	 *
	 * @param function a function to map <i>left</i> value to a new value
	 * @param <U>      a type of result of a new {@link Try}
	 * @return a {@link Try} with a mapped result
	 */
	@Contract(pure = true)
	public <U> @NotNull Try<U> map(@NotNull FunctionEx<T, U> function) {
		if (exception == null) {
			try {
				return new Try<>(function.apply(result), null);
			} catch (Exception ex) {
				handleError(ex, function);
				return new Try<>(null, ex);
			}
		}
		return mold();
	}

	/**
	 * Returns a mapped {@link Try} obtained by mapping a result
	 * of this {@link Try} to a new {@link Try}
	 *
	 * @param function a function to map a result of this {@link Try} a new {@link Try}
	 * @param <U>      a type of result of a new {@link Try}
	 * @return a mapped {@link Try}
	 */
	@Contract(pure = true)
	public <U> @NotNull Try<U> flatMap(@NotNull Function<T, Try<U>> function) {
		return exception == null ? function.apply(result) : mold();
	}

	/**
	 * Convert this {@link Try<T>} to an {@code Either<T, Exception>}
	 *
	 * @return an {@link Either} which represents either a result or an exception of this {@link Try}
	 */
	@Contract(pure = true)
	public @NotNull Either<T, Exception> toEither() {
		return exception == null ? Either.left(result) : Either.right(exception);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Try<?> other = (Try<?>) o;
		if (!Objects.equals(result, other.result)) return false;
		return Objects.equals(exception, other.exception);
	}

	@Override
	public int hashCode() {
		int hash = result != null ? result.hashCode() : 0;
		hash = 31 * hash + (exception != null ? exception.hashCode() : 0);
		return hash;
	}

	@Override
	public String toString() {
		return "{" + (isSuccess() ? "" + result : "" + exception) + "}";
	}
}
