package io.activej.common;

public final class Debug {
	public static final boolean LOG_ASYNC_CLOSE_ERRORS = ApplicationSettings.getBoolean(Debug.class, "logAsyncCloseErrors", false);
	public static final boolean STACKFUL_EXCEPTIONS = ApplicationSettings.getBoolean(Debug.class, "stackfulExceptions", false);
}
