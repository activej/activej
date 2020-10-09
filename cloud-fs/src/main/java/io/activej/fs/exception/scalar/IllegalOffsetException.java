package io.activej.fs.exception.scalar;

import io.activej.fs.exception.FsScalarException;
import org.jetbrains.annotations.NotNull;

public final class IllegalOffsetException extends FsScalarException {
	public IllegalOffsetException(@NotNull Class<?> component) {
		super(component, "Offset exceeds file size");
	}

	public IllegalOffsetException(@NotNull Class<?> component, @NotNull String message) {
		super(component, message);
	}
}
