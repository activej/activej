package io.activej.json;

public interface JsonGetter<T, V> {
	V get(T instance);
}
