package io.activej.serializer.datastream;

public class DeserializeException extends Exception {

	public DeserializeException() {
	}

	public DeserializeException(String message) {
		super(message);
	}

	public DeserializeException(String message, Throwable cause) {
		super(message, cause);
	}

	public DeserializeException(Throwable cause) {
		super(cause);
	}

	@Override
	public Throwable fillInStackTrace() {
		return this;
	}
}
