package io.activej.state.file;

public final class FileState<T> {
	private final T state;
	private final long revision;

	public FileState(T state, long revision) {
		this.state = state;
		this.revision = revision;
	}

	public long getRevision() {
		return revision;
	}

	public T getState() {
		return state;
	}
}
