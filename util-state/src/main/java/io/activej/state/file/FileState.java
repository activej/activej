package io.activej.state.file;

import org.jetbrains.annotations.NotNull;

public final class FileState<T> {
	private final @NotNull T state;
	private final long revision;

	public FileState(@NotNull T state, long revision) {
		this.state = state;
		this.revision = revision;
	}

	public long getRevision() {
		return revision;
	}

	public @NotNull T getState() {
		return state;
	}
}
