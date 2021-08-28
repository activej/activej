package io.activej.common.function;

import io.activej.common.exception.UncheckedException;

import java.util.function.Supplier;

@FunctionalInterface
public interface SupplierEx<T> {
	T get() throws Exception;

	static <T> SupplierEx<T> of(Supplier<T> uncheckedFn) {
		return () -> {
			try {
				return uncheckedFn.get();
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	static <T> Supplier<T> uncheckedOf(SupplierEx<T> checkedFn) {
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
