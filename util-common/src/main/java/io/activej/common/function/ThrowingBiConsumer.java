package io.activej.common.function;

@FunctionalInterface
public interface ThrowingBiConsumer<T, U> {
	void accept(T t, U u) throws Exception;
}
