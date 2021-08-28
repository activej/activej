package io.activej.common.function;

import io.activej.common.exception.UncheckedException;

import java.util.function.Supplier;

@FunctionalInterface
public interface ThrowingSupplier<T> {
	T get() throws Exception;

	static <T> ThrowingSupplier<T> of(Supplier<T> uncheckedFn) {
		return () -> {
			try {
				return uncheckedFn.get();
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	static <T> Supplier<T> uncheckedOf(ThrowingSupplier<T> checkedFn) {
		return () -> {
			try {
				return checkedFn.get();
			} catch (RuntimeException ex) {
				throw ex;
			} catch (Exception ex) {
				throw UncheckedException.of(ex);
			}
		};
	}
}
