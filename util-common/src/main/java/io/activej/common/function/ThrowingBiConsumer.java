package io.activej.common.function;

import io.activej.common.exception.UncheckedException;

import java.util.function.BiConsumer;

@FunctionalInterface
public interface ThrowingBiConsumer<T, U> {
	void accept(T t, U u) throws Exception;

	static <T, U> ThrowingBiConsumer<T, U> of(BiConsumer<T, U> fn) {
		return (t, u) -> {
			try {
				fn.accept(t, u);
			} catch (UncheckedException e) {
				throw e.getCause();
			}
		};
	}
}
