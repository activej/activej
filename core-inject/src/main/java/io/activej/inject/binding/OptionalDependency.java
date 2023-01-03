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

package io.activej.inject.binding;

import org.jetbrains.annotations.Nullable;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;

public final class OptionalDependency<T> {
	private static final OptionalDependency<?> EMPTY = new OptionalDependency<>(null);

	private final @Nullable T value;

	private OptionalDependency(@Nullable T value) {
		this.value = value;
	}

	public static <T> OptionalDependency<T> of(T value) {
		if (value instanceof OptionalDependency) {
			throw new IllegalArgumentException("Nested optional dependencies are not allowed");
		}

		return new OptionalDependency<>(value);
	}

	@SuppressWarnings("unchecked")
	public static <T> OptionalDependency<T> empty() {
		return (OptionalDependency<T>) EMPTY;
	}

	public boolean isPresent() {
		return value != null;
	}

	public T get() {
		if (value == null) {
			throw new NoSuchElementException();
		}
		return value;
	}

	public T orElse(T other) {
		return value != null ? value : other;
	}

	public T orElseGet(Supplier<? extends T> other) {
		return value != null ? value : other.get();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		OptionalDependency<?> that = (OptionalDependency<?>) o;

		return Objects.equals(value, that.value);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(value);
	}

	@Override
	public String toString() {
		return "OptionalDependency{" +
				(value == null ? "empty" : "value=" + value) +
				'}';
	}
}
