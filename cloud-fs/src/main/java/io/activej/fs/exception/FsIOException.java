package io.activej.fs.exception;

import org.jetbrains.annotations.NotNull;

public final class FsIOException extends FsException {
	public FsIOException(@NotNull Class<?> component, @NotNull String message) {
		super(component, message);
	}
}
