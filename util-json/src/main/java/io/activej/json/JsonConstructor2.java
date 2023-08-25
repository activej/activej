package io.activej.json;

public interface JsonConstructor2<T1, T2, R> {
	R create(T1 param1, T2 param2) throws JsonValidationException;
}
