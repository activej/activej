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

import io.activej.inject.Key;

import java.util.Objects;

/**
 * A simple POJO that combines a {@link Key} with booleans that indicate where
 * a key is required
 *
 * @see Binding
 */
public final class Dependency {
	private final Key<?> key;

	public Dependency(Key<?> key) {
		this.key = key;
	}

	public static Dependency toKey(Key<?> key) {
		return new Dependency(key);
	}

	public Key<?> getKey() {
		return key;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		Dependency that = (Dependency) o;

		return Objects.equals(key, that.key);
	}

	@Override
	public int hashCode() {
		return 31 * (key != null ? key.hashCode() : 0);
	}

	public String getDisplayString() {
		return key.getDisplayString();
	}

	@Override
	public String toString() {
		return key.toString();
	}
}
