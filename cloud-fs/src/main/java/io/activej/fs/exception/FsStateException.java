package io.activej.fs.exception;

import org.jetbrains.annotations.NotNull;

public final class FsStateException extends FsException {
	public FsStateException(@NotNull Class<?> component, @NotNull String message) {
		super(component, message);
	}
}
