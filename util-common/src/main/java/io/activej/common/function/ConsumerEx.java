package io.activej.common.function;

import io.activej.common.exception.UncheckedException;

import java.util.function.Consumer;

@FunctionalInterface
public interface ConsumerEx<T> {
	void accept(T t) throws Exception;

	static <T> ConsumerEx<T> of(Consumer<T> uncheckedFn) {
		return t -> {
			try {
				uncheckedFn.accept(t);
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	static <T> Consumer<T> uncheckedOf(ConsumerEx<T> checkedFn) {
		return t -> {
			try {
				checkedFn.accept(t);
			} catch (RuntimeException ex) {
				throw ex;
			} catch (Exception ex) {
				throw UncheckedException.of(ex);
			}
		};
	}
}
