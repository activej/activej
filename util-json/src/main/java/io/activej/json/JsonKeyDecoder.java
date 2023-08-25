package io.activej.json;

public interface JsonKeyDecoder<T> {
	T decode(String string) throws JsonValidationException;
}
