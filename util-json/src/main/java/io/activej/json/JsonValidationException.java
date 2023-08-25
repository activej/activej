package io.activej.json;

import java.io.IOException;

public class JsonValidationException extends IOException {
	public JsonValidationException() {
		super();
	}

	public JsonValidationException(String message) {
		super(message);
	}

	public JsonValidationException(String message, Throwable cause) {
		super(message, cause);
	}
}
