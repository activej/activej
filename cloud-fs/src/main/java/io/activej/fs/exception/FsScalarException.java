package io.activej.fs.exception;

import org.jetbrains.annotations.NotNull;

public class FsScalarException extends FsStateException {
	public FsScalarException(@NotNull Class<?> component, @NotNull String message) {
		super(component, message);
	}
}
