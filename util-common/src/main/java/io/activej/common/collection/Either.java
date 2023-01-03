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

import io.activej.common.recycle.Recyclers;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.function.*;

/**
 * A compound type that represents a disjoint union of two values (only one value is present, either left or right)
 * <p>
 * Supports {@code null} values
 *
 * @param <L> type of left value
 * @param <R> type of right value
 */
public final class Either<L, R> {
	static {
		Recyclers.register(Either.class, either -> {
			Recyclers.recycle(either.left);
			Recyclers.recycle(either.right);
		});
	}

	private final @Nullable L left;

	private final @Nullable R right;

	private final boolean isRight; // so that this either supports nulls

	private Either(@Nullable L left, @Nullable R right, boolean isRight) {
		this.left = left;
		this.right = right;
		this.isRight = isRight;
	}

	/**
	 * Creates a new {@link Either} with a <i>left</i> value
	 *
	 * @param left an object to be used as a <i>left</i> value
	 * @param <L>  type of left value
	 * @param <R>  type of right value
	 * @return a new instance of {@link Either} with a <i>left</i> value
	 */
	public static <L, R> Either<L, R> left(@Nullable L left) {
		return new Either<>(left, null, false);
	}

	/**
	 * Creates a new {@link Either} with a <i>right</i> value
	 *
	 * @param right an object to be used as a <i>right</i> value
	 * @param <L>   type of left value
	 * @param <R>   type of right value
	 * @return a new instance of {@link Either} with a <i>right</i> value
	 */
	public static <L, R> Either<L, R> right(@Nullable R right) {
		return new Either<>(null, right, true);
	}

	/**
	 * Returns whether this {@link Either} is <i>left</i> (has <i>left</i> value)
	 */
	@Contract(pure = true)
	public boolean isLeft() {
		return !isRight;
	}

	/**
	 * Returns whether this {@link Either} is <i>right</i> (has <i>right</i> value)
	 */
	@Contract(pure = true)
	public boolean isRight() {
		return isRight;
	}

	/**
	 * Returns a <i>left</i> value
	 */
	@Contract(pure = true)
	public @Nullable L getLeft() {
		return left;
	}

	/**
	 * Returns a <i>right</i> value
	 */
	@Contract(pure = true)
	public @Nullable R getRight() {
		return right;
	}

	/**
	 * If this {@link Either} is <i>left</i>, returns <i>left</i> value.
	 * Otherwise, returns a given default value
	 *
	 * @param defaultValue a default value to be returned if this {@link Either} is <i>right</i>
	 * @return a <i>left</i> value if this {@link Either} is <i>left</i> or a default value, otherwise
	 */
	@Contract(pure = true)
	public L getLeftElse(@Nullable L defaultValue) {
		return isLeft() ? left : defaultValue;
	}

	/**
	 * If this {@link Either} is <i>right</i>, returns <i>right</i> value.
	 * Otherwise, returns a given default value
	 *
	 * @param defaultValue a default value to be returned if this {@link Either} is <i>left</i>
	 * @return a <i>right</i> value if this {@link Either} is <i>right</i> or a default value, otherwise
	 */
	@Contract(pure = true)
	public R getRightElse(@Nullable R defaultValue) {
		return isRight() ? right : defaultValue;
	}

	/**
	 * If this {@link Either} is <i>left</i>, returns <i>left</i> value.
	 * Otherwise, returns a supplied default value
	 *
	 * @param defaultValueSupplier a supplier of default value to be obtained if this {@link Either} is <i>right</i>
	 * @return a <i>left</i> value if this {@link Either} is <i>left</i> or a supplied default value, otherwise
	 */
	@Contract(pure = true)
	public L getLeftElseGet(Supplier<? extends L> defaultValueSupplier) {
		return isLeft() ? left : defaultValueSupplier.get();
	}

	/**
	 * If this {@link Either} is <i>right</i>, returns <i>right</i> value.
	 * Otherwise, returns a supplied default value
	 *
	 * @param defaultValueSupplier a supplier of default value to be obtained if this {@link Either} is <i>left</i>
	 * @return a <i>right</i> value if this {@link Either} is <i>right</i> or a supplied default value, otherwise
	 */
	@Contract(pure = true)
	public R getRightElseGet(Supplier<? extends R> defaultValueSupplier) {
		return isRight() ? right : defaultValueSupplier.get();
	}

	/**
	 * Consumes a <i>left</i> value if this {@link Either} is <i>left</i>.
	 * Otherwise, does nothing
	 * <p>
	 * Always returns this {@link Either}
	 *
	 * @param leftConsumer a consumer of <i>left</i> value
	 * @return this {@link Either}
	 */
	@Contract(pure = true)
	public Either<L, R> ifLeft(Consumer<? super L> leftConsumer) {
		if (isLeft()) {
			leftConsumer.accept(left);
		}
		return this;
	}

	/**
	 * Consumes a <i>right</i> value if this {@link Either} is <i>right</i>.
	 * Otherwise, does nothing
	 * <p>
	 * Always returns this {@link Either}
	 *
	 * @param rightConsumer a consumer of <i>right</i> value
	 * @return this {@link Either}
	 */
	@Contract(pure = true)
	public Either<L, R> ifRight(Consumer<? super R> rightConsumer) {
		if (isRight()) {
			rightConsumer.accept(right);
		}
		return this;
	}

	/**
	 * Consumes both <i>left</i> and <i>right</i> values
	 * <p>
	 * Always returns this {@link Either}
	 *
	 * @param consumer a consumer of <i>left</i> and <i>right</i> values
	 * @return this {@link Either}
	 */
	@Contract(pure = true)
	public Either<L, R> consume(BiConsumer<? super L, ? super R> consumer) {
		consumer.accept(left, right);
		return this;
	}

	/**
	 * Consumes both <i>left</i> and <i>right</i> values
	 * <p>
	 * Always returns this {@link Either}
	 *
	 * @param leftConsumer  a consumer of <i>left</i> value
	 * @param rightConsumer a consumer of <i>right</i> value
	 * @return this {@link Either}
	 */
	@Contract(pure = true)
	public Either<L, R> consume(Consumer<? super L> leftConsumer, Consumer<? super R> rightConsumer) {
		if (isLeft()) {
			leftConsumer.accept(left);
		} else {
			rightConsumer.accept(right);
		}
		return this;
	}

	/**
	 * Applies a function to this {@link Either}'s <i>left</i> or <i>right</i> value
	 *
	 * @param leftFn  a function to map <i>left</i> value
	 * @param rightFn a function to map <i>left</i> value
	 * @param <U>     a type of mapping result
	 * @return a result of mapping of either <i>left</i> or <i>right</i> value
	 */
	@Contract(pure = true)
	public <U> U reduce(Function<? super L, ? extends U> leftFn, Function<? super R, ? extends U> rightFn) {
		return isLeft() ? leftFn.apply(left) : rightFn.apply(right);
	}

	/**
	 * Applies a function to this {@link Either}'s <i>left</i> and <i>right</i> values
	 *
	 * @param fn  a function to map <i>left</i> and <i>right</i> values
	 * @param <U> a type of mapping result
	 * @return a result of mapping of <i>left</i> and <i>right</i> values
	 */
	@Contract(pure = true)
	public <U> U reduce(BiFunction<? super L, ? super R, ? extends U> fn) {
		return fn.apply(left, right);
	}

	/**
	 * Returns a new {@link Either} which has its <i>left</i> and <i>right</i> values swapped
	 *
	 * @return a swapped {@link Either}
	 */
	@Contract(pure = true)
	public Either<R, L> swap() {
		return new Either<>(right, left, !isRight);
	}

	/**
	 * Returns a mapped {@link Either} obtained by mapping a <i>left</i> value
	 * of this {@link Either} to a new value
	 *
	 * @param fn  a function to map <i>left</i> value to a new value
	 * @param <T> a type of new <i>left</i> value
	 * @return an {@link Either} with a mapped <i>left</i> value
	 */
	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	public <T> Either<T, R> mapLeft(Function<? super L, ? extends T> fn) {
		return isLeft() ?
				new Either<>(fn.apply(left), null, false) :
				(Either<T, R>) this;
	}

	/**
	 * Returns a mapped {@link Either} obtained by mapping a <i>right</i> value
	 * of this {@link Either} to a new value
	 *
	 * @param fn  a function to map <i>right</i> value to a new value
	 * @param <T> a type of new <i>right</i> value
	 * @return an {@link Either} with a mapped <i>right</i> value
	 */
	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	public <T> Either<L, T> mapRight(Function<? super R, ? extends T> fn) {
		return isRight() ?
				new Either<>(null, fn.apply(right), true) :
				(Either<L, T>) this;
	}

	/**
	 * Returns a mapped {@link Either} obtained by mapping a <i>left</i> value
	 * of this {@link Either} to a new {@link Either} which has the same <i>right</i> type
	 *
	 * @param fn  a function to map <i>left</i> value to a new {@link Either}
	 * @param <T> a type of new <i>left</i> value
	 * @return a mapped {@link Either}
	 */
	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	public <T> Either<T, R> flatMapLeft(Function<? super L, Either<T, R>> fn) {
		return isLeft() ?
				fn.apply(left) :
				(Either<T, R>) this;
	}

	/**
	 * Returns a mapped {@link Either} obtained by mapping a <i>right</i> value
	 * of this {@link Either} to a new {@link Either} which has the same <i>left</i> type
	 *
	 * @param fn  a function to map <i>right</i> value to a new {@link Either}
	 * @param <T> a type of new <i>right</i> value
	 * @return a mapped {@link Either}
	 */
	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	public <T> Either<L, T> flatMapRight(Function<? super R, Either<L, T>> fn) {
		return isRight() ?
				fn.apply(right) :
				(Either<L, T>) this;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Either<?, ?> either = (Either<?, ?>) o;
		return Objects.equals(left, either.left) && Objects.equals(right, either.right);
	}

	@Override
	public int hashCode() {
		int result = 0;
		result = 31 * result + (left != null ? left.hashCode() : 0);
		result = 31 * result + (right != null ? right.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "{" + (isLeft() ? "left=" + left : "right=" + right) + "}";
	}
}
