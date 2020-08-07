package io.activej.fs.exception.scalar;

import io.activej.fs.exception.FsScalarException;
import org.jetbrains.annotations.NotNull;

public final class IsADirectoryException extends FsScalarException {
	public IsADirectoryException(@NotNull Class<?> component) {
		super(component, "Operated file is a directory");
	}

	public IsADirectoryException(@NotNull Class<?> component, @NotNull String message) {
		super(component, message);
	}
}
