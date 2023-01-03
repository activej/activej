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

import io.activej.inject.Key;
import io.activej.inject.Scope;

import java.util.Arrays;

import static io.activej.inject.Scope.UNSCOPED;
import static io.activej.inject.util.Utils.getScopeDisplayString;

/**
 * This is a simple generic POJO (or POGJO) for a {@link Key} with associated scope path.
 */
public final class ScopedKey {
	private final Scope[] scope;
	private final Key<?> key;

	private ScopedKey(Scope[] scope, Key<?> key) {
		this.scope = scope;
		this.key = key;
	}

	public static ScopedKey of(Key<?> key) {
		return new ScopedKey(UNSCOPED, key);
	}

	public static ScopedKey of(Scope scope, Key<?> key) {
		return new ScopedKey(new Scope[]{scope}, key);
	}

	public static ScopedKey of(Scope[] scope, Key<?> key) {
		return new ScopedKey(scope.length != 0 ? scope : UNSCOPED, key);
	}

	public Scope[] getScope() {

		return scope;
	}

	public Key<?> getKey() {
		return key;
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
		ScopedKey other = (ScopedKey) o;
		return Arrays.equals(scope, other.scope) && key.equals(other.key);

	}

	@Override
	public int hashCode() {
		return 31 * Arrays.hashCode(scope) + key.hashCode();
	}

	@Override
	public String toString() {
		return getScopeDisplayString(scope) + " " + key.toString();
	}
}
