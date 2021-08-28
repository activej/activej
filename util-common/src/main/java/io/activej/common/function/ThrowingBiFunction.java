package io.activej.common.function;

import io.activej.common.exception.UncheckedException;

import java.util.function.BiFunction;

@FunctionalInterface
public interface ThrowingBiFunction<T, U, R> {
	R apply(T t, U u) throws Exception;

	static <T, U, R> ThrowingBiFunction<T, U, R> of(BiFunction<T, U, R> uncheckedFn) {
		return (t, u) -> {
			try {
				return uncheckedFn.apply(t, u);
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	static <T, U, R> BiFunction<T, U, R> uncheckedOf(ThrowingBiFunction<T, U, R> checkedFn) {
		return (t, u) -> {
			try {
				return checkedFn.apply(t, u);
			} catch (RuntimeException ex) {
				throw ex;
			} catch (Exception ex) {
				throw UncheckedException.of(ex);
			}
		};
	}
}
