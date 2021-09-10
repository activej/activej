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
 * a key is required and whether it is implicit
 *
 * @see Binding
 */
public final class Dependency {
	private final Key<?> key;
	private final boolean required;
	private final boolean implicit;

	public Dependency(Key<?> key, boolean required, boolean implicit) {
		this.key = key;
		this.required = required;
		this.implicit = implicit;
	}

	public static Dependency toKey(Key<?> key) {
		return new Dependency(key, true, false);
	}

	public static Dependency toKey(Key<?> key, boolean required) {
		return new Dependency(key, required, false);
	}

	public static Dependency toOptionalKey(Key<?> key) {
		return new Dependency(key, false, false);
	}

	/**
	 * Implicit dependencies do not cause cycle-check errors and are drawn in gray in debug graphviz output.
	 * Such dependencies <b>SHOULD NOT</b> be instantiated since they may cause various cycle-related errors,
	 * such infinite recursion.
	 * <p>
	 * They are used to describe some logical dependency that may or may not be cyclic
	 */
	public static Dependency implicit(Key<?> key, boolean required) {
		return new Dependency(key, required, true);
	}

	public Key<?> getKey() {
		return key;
	}

	public boolean isRequired() {
		return required;
	}

	public boolean isImplicit() {
		return implicit;
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

		return required == that.required && Objects.equals(key, that.key);
	}

	@Override
	public int hashCode() {
		return 31 * (key != null ? key.hashCode() : 0) + (required ? 1 : 0);
	}

	public String getDisplayString() {
		return (required ? "" : "optional ") + key.getDisplayString();
	}

	@Override
	public String toString() {
		return (required ? "" : "optional ") + key;
	}
}
