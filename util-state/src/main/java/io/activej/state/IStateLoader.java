package io.activej.state;

import java.io.IOException;

public interface IStateLoader<R extends Comparable<R>, T> {
	boolean hasDiffsSupport();

	R getLastSnapshotRevision() throws IOException;

	R getLastDiffRevision(R currentRevision) throws IOException;

	T loadSnapshot(R revision) throws IOException;

	T loadDiff(T state, R revisionFrom, R revisionTo) throws IOException;
}
