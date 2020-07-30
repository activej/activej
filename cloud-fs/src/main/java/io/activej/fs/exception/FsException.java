package io.activej.fs.exception;

import io.activej.common.exception.StacklessException;
import org.jetbrains.annotations.NotNull;

public class FsException extends StacklessException {
	public FsException(@NotNull Class<?> component, @NotNull String message) {
		super(component, message);
	}

	public FsException(@NotNull Class<?> component, @NotNull String message, @NotNull Throwable cause) {
		super(component, message, cause);
	}
}
