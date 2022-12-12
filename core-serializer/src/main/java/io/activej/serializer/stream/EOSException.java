package io.activej.serializer.stream;

import io.activej.common.ApplicationSettings;

import java.io.IOException;

public final class EOSException extends IOException {
	public static final boolean WITH_STACK_TRACE = ApplicationSettings.getBoolean(EOSException.class, "withStackTrace", false);

	public EOSException() {
		super("Unexpected end of stream");
	}

	public EOSException(String message) {
		super(message);
	}

	@Override
	public Throwable fillInStackTrace() {
		return WITH_STACK_TRACE ? fillInStackTrace() : this;
	}
}
