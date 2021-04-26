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

package io.activej.inject.util;

import io.activej.inject.Scope;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.activej.inject.Scope.UNSCOPED;
import static io.activej.inject.util.Utils.getScopeDisplayString;

/**
 * This is a simple generic POJO (or POGJO) for some object with associated scope path.
 * <p>
 * It is generic only because it is used as both ScopedValue&lt;Key&lt;?&gt;&gt;, and ScopedValue&lt;Dependency&gt;.
 */
public final class ScopedValue<T> {
	private final Scope[] scope;
	private final T value;

	private ScopedValue(Scope[] scope, T value) {
		this.scope = scope;
		this.value = value;
	}

	public static <T> ScopedValue<T> of(@NotNull T value) {
		return new ScopedValue<>(UNSCOPED, value);
	}

	public static <T> ScopedValue<T> of(@NotNull Scope scope, @NotNull T value) {
		return new ScopedValue<>(new Scope[]{scope}, value);
	}

	public static <T> ScopedValue<T> of(Scope[] scope, @NotNull T value) {
		return new ScopedValue<>(scope.length != 0 ? scope : UNSCOPED, value);
	}

	public Scope[] getScope() {

		return scope;
	}

	@NotNull
	public T get() {
		return value;
	}

	public boolean isScoped() {
		return scope.length != 0;
	}

	public boolean isUnscoped() {
		return scope.length == 0;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ScopedValue<?> other = (ScopedValue<?>) o;
		return Arrays.equals(scope, other.scope) && value.equals(other.value);

	}

	@Override
	public int hashCode() {
		return 31 * Arrays.hashCode(scope) + value.hashCode();
	}

	@Override
	public String toString() {
		return getScopeDisplayString(scope) + " " + value.toString();
	}
}
