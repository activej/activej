package io.activej.state;

import io.activej.common.annotation.ComponentInterface;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

@ComponentInterface
public interface IStateManager<T, R extends Comparable<R>> {
	R newRevision() throws IOException;

	@Nullable R getLastSnapshotRevision() throws IOException;

	@Nullable R getLastDiffRevision(R currentRevision) throws IOException;

	T loadSnapshot(R revision) throws IOException;

	@Nullable T tryLoadSnapshot(R revision) throws IOException;

	T loadDiff(T state, R revisionFrom, R revisionTo) throws IOException;

	@Nullable T tryLoadDiff(T state, R revisionFrom, R revisionTo) throws IOException;

	void saveSnapshot(T state, R revision) throws IOException;

	void saveDiff(T state, R revision, T stateFrom, R revisionFrom) throws IOException;
}
