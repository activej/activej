package io.activej.fs.exception.scalar;

import io.activej.fs.exception.FsScalarException;
import org.jetbrains.annotations.NotNull;

public final class PathContainsFileException extends FsScalarException {
	public PathContainsFileException(@NotNull Class<?> component) {
		super(component, "Path contains existing file as its part");
	}

	public PathContainsFileException(@NotNull Class<?> component, @NotNull String message) {
		super(component, message);
	}
}
