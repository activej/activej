package io.activej.fs.exception.scalar;

import io.activej.fs.exception.FsScalarException;
import org.jetbrains.annotations.NotNull;

public final class FileNotFoundException extends FsScalarException {
	public FileNotFoundException(@NotNull Class<?> component) {
		super(component, "File not found");
	}

	public FileNotFoundException(@NotNull Class<?> component, @NotNull String message) {
		super(component, message);
	}
}
