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

import io.activej.common.exception.UncheckedException;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.*;
import java.util.stream.Collector;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;

public final class Try<T> {
	private final T result;

	@Nullable
	private final Throwable throwable;

	private Try(@Nullable T result, @Nullable Throwable e) {
		this.result = result;
		this.throwable = e;
	}

	public static <T> Try<T> of(@Nullable T result) {
		return new Try<>(result, null);
	}

	public static <T> Try<T> of(@Nullable T result, @Nullable Throwable e) {
		checkArgument(result == null || e == null, "Either result or exception should be null");
		return new Try<>(result, e);
	}

	public static <T> Try<T> ofException(@NotNull Throwable e) {
		return new Try<>(null, e);
	}

	public static <T> Try<T> wrap(@NotNull Supplier<T> computation) {
		try {
			return new Try<>(computation.get(), null);
		} catch (UncheckedException u) {
			return new Try<>(null, u.getCause());
		}
	}

	public static <T> Try<T> wrap(@NotNull Runnable computation) {
		try {
			computation.run();
			return new Try<>(null, null);
		} catch (UncheckedException u) {
			return new Try<>(null, u.getCause());
		}
	}

	public static <T> Try<T> wrap(@NotNull Callable<? extends T> computation) {
		try {
			@Nullable T result = computation.call();
			return new Try<>(result, null);
		} catch (UncheckedException u) {
			return new Try<>(null, u.getCause());
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			return new Try<>(null, e);
		}
	}

	public static Collector<Try<Void>, ?, Try<Void>> voidReducer() {
		return reducer(($1, $2) -> null);
	}

	public static <T> Collector<Try<T>, ?, Try<T>> reducer(@NotNull BinaryOperator<T> combiner) {
		return reducer(null, combiner);
	}

	public static <T> Collector<Try<T>, ?, Try<T>> reducer(@Nullable T identity, @NotNull BinaryOperator<T> combiner) {
		class Accumulator {
			T result = identity;
			final List<Throwable> throwables = new ArrayList<>();
		}
		return Collector.of(Accumulator::new,
				(acc, t) -> {
					if (t.isSuccess()) {
						acc.result = acc.result != null ? combiner.apply(acc.result, t.get()) : t.get();
					} else {
						acc.throwables.add(t.getException());
					}
				},
				(acc1, acc2) -> {
					acc1.result = combiner.apply(acc1.result, acc2.result);
					acc1.throwables.addAll(acc2.throwables);
					return acc1;
				},
				acc -> {
					if (acc.throwables.isEmpty()) {
						return Try.of(acc.result);
					}
					Throwable e = acc.throwables.get(0);
					for (Throwable t : acc.throwables) {
						if (t != e) {
							e.addSuppressed(t);
						}
					}
					return Try.ofException(e);
				});
	}

	@Contract(pure = true)
	public boolean isSuccess() {
		return throwable == null;
	}

	@Contract(pure = true)
	public boolean isException() {
		return throwable != null;
	}

	@Contract(pure = true)
	public T get() {
		checkState(isSuccess());
		return result;
	}

	@Contract(pure = true)
	public T getOrThrow() throws Exception {
		if (throwable == null) {
			return result;
		}
		throw throwable instanceof Exception ? (Exception) throwable : new RuntimeException(throwable);
	}

	@Contract(pure = true)
	public T getOr(@Nullable T defaultValue) {
		return throwable == null ? result : defaultValue;
	}

	@Contract(pure = true)
	public T getOrSupply(@NotNull Supplier<? extends T> defaultValueSupplier) {
		return throwable == null ? result : defaultValueSupplier.get();
	}

	@Contract(pure = true)
	@Nullable
	public T getOrNull() {
		return result;
	}

	@Contract(pure = true)
	@NotNull
	public Throwable getException() {
		checkState(isException());
		return throwable;
	}

	@Contract(pure = true)
	@Nullable
	public Throwable getExceptionOrNull() {
		return throwable;
	}

	@NotNull
	public Try<T> ifSuccess(@NotNull Consumer<? super T> resultConsumer) {
		if (isSuccess()) {
			resultConsumer.accept(result);
		}
		return this;
	}

	@NotNull
	public Try<T> ifException(@NotNull Consumer<Throwable> exceptionConsumer) {
		if (isException()) {
			exceptionConsumer.accept(throwable);
		}
		return this;
	}

	@NotNull
	public Try<T> consume(@NotNull BiConsumer<? super T, Throwable> consumer) {
		consumer.accept(result, throwable);
		return this;
	}

	@NotNull
	public Try<T> consume(@NotNull Consumer<? super T> resultConsumer, @NotNull Consumer<Throwable> exceptionConsumer) {
		if (isSuccess()) {
			resultConsumer.accept(result);
		} else {
			exceptionConsumer.accept(throwable);
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
	public <U> U reduce(@NotNull Function<? super T, ? extends U> function, @NotNull Function<Throwable, ? extends U> exceptionFunction) {
		return throwable == null ? function.apply(result) : exceptionFunction.apply(throwable);
	}

	@Contract(pure = true)
	public <U> U reduce(@NotNull BiFunction<? super T, Throwable, ? extends U> fn) {
		return fn.apply(result, throwable);
	}

	@Contract(pure = true)
	@NotNull
	public <U> Try<U> map(@NotNull Function<T, U> function) {
		if (throwable == null) {
			try {
				return new Try<>(function.apply(result), null);
			} catch (UncheckedException u) {
				return new Try<>(null, u.getCause());
			}
		}
		return mold();
	}

	@Contract(pure = true)
	@NotNull
	public <U> Try<U> flatMap(@NotNull Function<T, Try<U>> function) {
		return throwable == null ? function.apply(result) : mold();
	}

	@Contract(pure = true)
	@NotNull
	public Either<T, Throwable> toEither() {
		return throwable == null ? Either.left(result) : Either.right(throwable);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Try<?> other = (Try<?>) o;
		if (!Objects.equals(result, other.result)) return false;
		return Objects.equals(throwable, other.throwable);
	}

	@Override
	public int hashCode() {
		int hash = result != null ? result.hashCode() : 0;
		hash = 31 * hash + (throwable != null ? throwable.hashCode() : 0);
		return hash;
	}

	@Override
	public String toString() {
		return "{" + (isSuccess() ? "" + result : "" + throwable) + "}";
	}
}
