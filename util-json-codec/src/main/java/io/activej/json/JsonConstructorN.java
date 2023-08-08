package io.activej.json;

public interface JsonConstructorN<R> {
	R create(Object[] params) throws JsonValidationException;
}
