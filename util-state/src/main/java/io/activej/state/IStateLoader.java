package io.activej.state;

import org.jetbrains.annotations.Nullable;

import java.io.IOException;

public interface IStateLoader<R extends Comparable<R>, T> {
	boolean hasDiffsSupport();

	@Nullable R getLastSnapshotRevision() throws IOException;

	@Nullable R getLastDiffRevision(R currentRevision) throws IOException;

	T loadSnapshot(R revision) throws IOException;

	T loadDiff(T state, R revisionFrom, R revisionTo) throws IOException;

	StateWithRevision<R, T> load() throws IOException;

	StateWithRevision<R, T> load(T stateFrom, R revisionFrom) throws IOException;

	record StateWithRevision<R, T>(R revision, T state) {
	}
}
