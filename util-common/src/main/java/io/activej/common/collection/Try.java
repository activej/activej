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

public final class Try<T> {
	static {
		Recyclers.register(Try.class, aTry -> Recyclers.recycle(aTry.result));
	}

	private final T result;

	@Nullable
	private final Exception exception;

	private Try(@Nullable T result, @Nullable Exception e) {
		this.result = result;
		this.exception = e;
	}

	public static <T> Try<T> of(@Nullable T result) {
		return new Try<>(result, null);
	}

	public static <T> Try<T> of(@Nullable T result, @Nullable Exception e) {
		checkArgument(result == null || e == null, "Either result or exception should be null");
		return new Try<>(result, e);
	}

	public static <T> Try<T> ofException(@NotNull Exception e) {
		return new Try<>(null, e);
	}

	public static <T> Try<T> wrap(@NotNull SupplierEx<T> computation) {
		try {
			return new Try<>(computation.get(), null);
		} catch (RuntimeException ex) {
			throw ex;
		} catch (Exception ex) {
			return new Try<>(null, ex);
		}
	}

	public static <T> Try<T> wrap(@NotNull RunnableEx computation) {
		try {
			computation.run();
			return new Try<>(null, null);
		} catch (RuntimeException ex) {
			throw ex;
		} catch (Exception ex) {
			return new Try<>(null, ex);
		}
	}

	public static <T> Try<T> wrap(@NotNull Callable<? extends T> computation) {
		try {
			@Nullable T result = computation.call();
			return new Try<>(result, null);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			return new Try<>(null, e);
		}
	}

	@Contract(pure = true)
	public boolean isSuccess() {
		return exception == null;
	}

	@Contract(pure = true)
	public boolean isException() {
		return exception != null;
	}

	@Contract(pure = true)
	@Nullable
	public T get() {
		return result;
	}

	@Contract(pure = true)
	public T getElse(@Nullable T defaultValue) {
		return exception == null ? result : defaultValue;
	}

	@Contract(pure = true)
	public T getElseGet(@NotNull Supplier<? extends T> defaultValueSupplier) {
		return exception == null ? result : defaultValueSupplier.get();
	}

	@Contract(pure = true)
	@Nullable
	public Exception getException() {
		return exception;
	}

	@NotNull
	public Try<T> ifSuccess(@NotNull Consumer<? super T> resultConsumer) {
		if (isSuccess()) {
			resultConsumer.accept(result);
		}
		return this;
	}

	@NotNull
	public Try<T> ifException(@NotNull Consumer<Exception> exceptionConsumer) {
		if (isException()) {
			exceptionConsumer.accept(exception);
		}
		return this;
	}

	@NotNull
	public Try<T> consume(@NotNull BiConsumer<? super T, Exception> consumer) {
		consumer.accept(result, exception);
		return this;
	}

	@NotNull
	public Try<T> consume(@NotNull Consumer<? super T> resultConsumer, @NotNull Consumer<Exception> exceptionConsumer) {
		if (isSuccess()) {
			resultConsumer.accept(result);
		} else {
			exceptionConsumer.accept(exception);
		}
		return this;
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	private <U> Try<U> mold() {
		checkState(isException());
		return (Try<U>) this;
	}

	@Contract(pure = true)
	public <U> U reduce(@NotNull Function<? super T, ? extends U> function, @NotNull Function<Exception, ? extends U> exceptionFunction) {
		return exception == null ? function.apply(result) : exceptionFunction.apply(exception);
	}

	@Contract(pure = true)
	public <U> U reduce(@NotNull BiFunction<? super T, Exception, ? extends U> fn) {
		return fn.apply(result, exception);
	}

	@Contract(pure = true)
	@NotNull
	public <U> Try<U> map(@NotNull FunctionEx<T, U> function) {
		if (exception == null) {
			try {
				return new Try<>(function.apply(result), null);
			} catch (RuntimeException ex) {
				throw ex;
			} catch (Exception ex) {
				return new Try<>(null, ex);
			}
		}
		return mold();
	}

	@Contract(pure = true)
	@NotNull
	public <U> Try<U> flatMap(@NotNull Function<T, Try<U>> function) {
		return exception == null ? function.apply(result) : mold();
	}

	@Contract(pure = true)
	@NotNull
	public Either<T, Exception> toEither() {
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
