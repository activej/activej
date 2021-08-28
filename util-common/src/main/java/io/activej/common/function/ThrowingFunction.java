package io.activej.common.function;

import io.activej.common.exception.UncheckedException;

import java.util.function.Function;

@FunctionalInterface
public interface ThrowingFunction<T, R> {
	R apply(T t) throws Exception;

	static <T, R> ThrowingFunction<T, R> of(Function<T, R> fn) {
		return t -> {
			try {
				return fn.apply(t);
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	static <T, R> Function<T, R> uncheckedOf(ThrowingFunction<T, R> checkedFn) {
		return t -> {
			try {
				return checkedFn.apply(t);
			} catch (RuntimeException ex) {
				throw ex;
			} catch (Exception ex) {
				throw UncheckedException.of(ex);
			}
		};
	}
}
