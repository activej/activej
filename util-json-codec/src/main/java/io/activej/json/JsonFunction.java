package io.activej.json;

public interface JsonFunction<T, R> {
	R apply(T param) throws JsonValidationException;
}
