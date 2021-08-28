package io.activej.common.function;

import io.activej.common.exception.UncheckedException;

@FunctionalInterface
public interface RunnableEx {
	void run() throws Exception;

	static RunnableEx of(Runnable uncheckedFn) {
		return () -> {
			try {
				uncheckedFn.run();
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	static Runnable uncheckedOf(RunnableEx checkedFn) {
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
