package io.activej.state.file;

public record FileState<T>(T state, long revision) {
}
