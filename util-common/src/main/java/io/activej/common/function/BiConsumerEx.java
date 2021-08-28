package io.activej.common.function;

import io.activej.common.exception.UncheckedException;

import java.util.function.BiConsumer;

@FunctionalInterface
public interface BiConsumerEx<T, U> {
	void accept(T t, U u) throws Exception;

	static <T, U> BiConsumerEx<T, U> of(BiConsumer<T, U> uncheckedFn) {
		return (t, u) -> {
			try {
				uncheckedFn.accept(t, u);
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	static <T, U> BiConsumer<T, U> uncheckedOf(BiConsumerEx<T, U> checkedFn) {
		return (t, u) -> {
			try {
				checkedFn.accept(t, u);
			} catch (RuntimeException ex) {
				throw ex;
			} catch (Exception ex) {
				throw UncheckedException.of(ex);
			}
		};
	}
}
