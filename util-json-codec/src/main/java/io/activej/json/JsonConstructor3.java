package io.activej.json;

public interface JsonConstructor3<T1, T2, T3, R> {
	R create(T1 param1, T2 param2, T3 param3) throws JsonValidationException;
}
