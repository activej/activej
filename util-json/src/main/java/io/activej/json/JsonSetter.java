package io.activej.json;

public interface JsonSetter<T, V> {
	void set(T instance, V value) throws JsonValidationException;
}
