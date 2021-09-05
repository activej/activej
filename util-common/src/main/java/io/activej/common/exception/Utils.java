package io.activej.common.exception;

import io.activej.common.ApplicationSettings;

public class Utils {
	public static final boolean PROPAGATE_RUNTIME_EXCEPTIONS = ApplicationSettings.getBoolean(RuntimeException.class, "propagateRuntimeExceptions", true);

	public static void propagateRuntimeException(Exception ex) throws RuntimeException {
		if (PROPAGATE_RUNTIME_EXCEPTIONS && ex instanceof RuntimeException) {
			throw (RuntimeException) ex;
		}
	}
}
