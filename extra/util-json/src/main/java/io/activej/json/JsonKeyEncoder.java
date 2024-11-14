package io.activej.json;

public interface JsonKeyEncoder<T> {
	String encode(T value);
}
