package io.activej.fs.exception.scalar;

import io.activej.fs.exception.FsScalarException;
import org.jetbrains.annotations.NotNull;

public final class IllegalOffsetException extends FsScalarException {
	private final long fileSize;

	public IllegalOffsetException(@NotNull Class<?> component, long fileSize) {
		super(component, "Offset exceeds file size");
		this.fileSize = fileSize;
	}

	public IllegalOffsetException(@NotNull Class<?> component, long fileSize, @NotNull String message) {
		super(component, message);
		this.fileSize = fileSize;
	}

	public long getFileSize() {
		return fileSize;
	}

	@Override
	public String getMessage() {
		return super.getMessage() + " [size=" + fileSize + ']';
	}
}
