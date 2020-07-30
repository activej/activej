package io.activej.fs.exception;

import org.jetbrains.annotations.NotNull;

public final class FsIOException extends FsException {
	public FsIOException(@NotNull Class<?> component, @NotNull String message) {
		super(component, message);
	}

	public FsIOException(@NotNull Class<?> component, @NotNull Throwable cause) {
		super(component, cause.getMessage());
	}

	public FsIOException(@NotNull Class<?> component, @NotNull String message, @NotNull Throwable cause) {
		super(component, message, cause);
	}
}
