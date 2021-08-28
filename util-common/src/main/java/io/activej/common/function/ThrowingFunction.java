package io.activej.common.function;

@FunctionalInterface
public interface ThrowingFunction<T, R> {
	R apply(T t) throws Exception;
}
