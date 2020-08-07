package io.activej.fs.exception.scalar;

import io.activej.fs.exception.FsScalarException;
import org.jetbrains.annotations.NotNull;

public final class ForbiddenPathException extends FsScalarException {
	public ForbiddenPathException(@NotNull Class<?> component) {
		super(component, "Forbidden path");
	}

	public ForbiddenPathException(@NotNull Class<?> component, @NotNull String message) {
		super(component, message);
	}
}
