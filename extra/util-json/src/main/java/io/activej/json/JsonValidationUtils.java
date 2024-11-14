package io.activej.json;

import org.jetbrains.annotations.Nullable;

public class JsonValidationUtils {

	public static <T> T validateNotNull(@Nullable T reference) throws JsonValidationException {
		if (reference != null) {
			return reference;
		}
		throw new JsonValidationException();
	}

	public static void validateArgument(boolean expression) throws JsonValidationException {
		if (!expression) {
			throw new JsonValidationException();
		}
	}

}
