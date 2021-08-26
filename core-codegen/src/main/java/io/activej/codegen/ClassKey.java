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

package io.activej.codegen;

import java.util.Arrays;

/**
 * A key that is used as a cache key in a {@link DefiningClassLoader}.
 * <p>
 * Represents some superclass with an array of parameters.
 */
public final class ClassKey<T> {
	private final Class<T> clazz;
	private final Object[] parameters;

	private ClassKey(Class<T> clazz, Object[] parameters) {
		this.clazz = clazz;
		this.parameters = parameters;
	}

	public static <T> ClassKey<T> of(Class<? super T> clazz, Object... parameters) {
		//noinspection unchecked,rawtypes
		return new ClassKey<T>((Class) clazz, parameters);
	}

	public Class<T> getKeyClass() {
		return clazz;
	}

	public Object[] getParameters() {
		return parameters;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ClassKey<?> that = (ClassKey<?>) o;
		return clazz.equals(that.clazz) &&
				Arrays.equals(this.parameters, that.parameters);
	}

	@Override
	public int hashCode() {
		return clazz.hashCode() * 31 + Arrays.hashCode(parameters);
	}

	@Override
	public String toString() {
		return clazz.getName() +
				(parameters != null && parameters.length != 0 ? " " + Arrays.toString(parameters) : "");
	}
}
