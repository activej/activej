package io.activej.state;

import org.jetbrains.annotations.Nullable;

import java.io.IOException;

public interface IStateLoader<R extends Comparable<R>, T> {
	record StateWithRevision<R, T>(R revision, T state) {}

	@Nullable StateWithRevision<R, T> load() throws IOException;

	@Nullable StateWithRevision<R, T> load(T stateFrom, R revisionFrom) throws IOException;
}
