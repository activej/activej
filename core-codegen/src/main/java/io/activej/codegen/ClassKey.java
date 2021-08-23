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
