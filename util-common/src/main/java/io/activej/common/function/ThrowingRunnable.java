package io.activej.common.function;

import io.activej.common.exception.UncheckedException;

@FunctionalInterface
public interface ThrowingRunnable {
	void run() throws Exception;

	static ThrowingRunnable of(Runnable uncheckedFn) {
		return () -> {
			try {
				uncheckedFn.run();
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	static Runnable uncheckedOf(ThrowingRunnable checkedFn) {
		return () -> {
			try {
				checkedFn.run();
			} catch (RuntimeException ex) {
				throw ex;
			} catch (Exception ex) {
				throw UncheckedException.of(ex);
			}
		};
	}
}
