package io.activej.codegen;

import java.util.Arrays;

public final class ClassKey<T> {
	private final Class<T> clazz;
	private final Object[] parameters;

	private ClassKey(Class<T> clazz, Object[] parameters) {
		this.clazz = clazz;
		this.parameters = parameters;
	}

	public static <T> ClassKey<T> of(Class<T> clazz, Object... parameters) {
		return new ClassKey<T>(clazz, parameters);
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
		ClassKey<?> key = (ClassKey<?>) o;
		return clazz.equals(key.clazz) &&
				Arrays.equals(this.parameters, key.parameters);
	}

	@Override
	public int hashCode() {
		int result = clazz.hashCode();
		result = 31 * result + Arrays.hashCode(parameters);
		return result;
	}

	@Override
	public String toString() {
		return clazz.getName() +
				(parameters != null && parameters.length != 0 ? " " + Arrays.toString(parameters) : "");
	}
}
