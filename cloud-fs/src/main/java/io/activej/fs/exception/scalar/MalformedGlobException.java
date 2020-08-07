package io.activej.fs.exception.scalar;

import io.activej.fs.exception.FsScalarException;
import org.jetbrains.annotations.NotNull;

public final class MalformedGlobException extends FsScalarException {
	public MalformedGlobException(@NotNull Class<?> component) {
		super(component, "Malformed glob");
	}

	public MalformedGlobException(@NotNull Class<?> component, @NotNull String message) {
		super(component, message);
	}
}
