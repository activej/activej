package io.activej.state;

import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Objects;
import java.util.SortedSet;

public interface IStateLoader<R extends Comparable<R>, T> {
	boolean hasDiffsSupport();

	SortedSet<R> listSnapshotRevisions() throws IOException;

	SortedSet<R> listDiffRevisions(R currentRevision) throws IOException;

	default @Nullable R getLastSnapshotRevision() throws IOException {
		SortedSet<R> snapshotRevisions = listSnapshotRevisions();
		return snapshotRevisions.isEmpty() ? null : snapshotRevisions.last();
	}

	default @Nullable R getLastDiffRevision(R currentRevision) throws IOException {
		if (!hasDiffsSupport()) throw new UnsupportedOperationException();
		SortedSet<R> diffRevisions = listDiffRevisions(currentRevision);
		return diffRevisions.isEmpty() ? null : diffRevisions.last();
	}

	T loadSnapshot(R revision) throws IOException;

	T loadDiff(T state, R revisionFrom, R revisionTo) throws IOException;

	default StateWithRevision<R, T> load() throws IOException {
		R lastRevision = getLastSnapshotRevision();
		if (lastRevision == null) throw new IOException("State is empty");

		return new StateWithRevision<>(lastRevision, loadSnapshot(lastRevision));
	}

	default StateWithRevision<R, T> load(T stateFrom, R revisionFrom) throws IOException {
		R lastRevision = getLastSnapshotRevision();
		if (Objects.equals(revisionFrom, lastRevision)) {
			return new StateWithRevision<>(revisionFrom, stateFrom);
		}

		if (hasDiffsSupport()) {
			R lastDiffRevision = getLastDiffRevision(revisionFrom);
			if (lastDiffRevision != null && (lastRevision == null || lastDiffRevision.compareTo(lastRevision) >= 0)) {
				T state = loadDiff(stateFrom, revisionFrom, lastDiffRevision);
				return new StateWithRevision<>(lastDiffRevision, state);
			}
		}

		if (lastRevision == null) throw new IOException("State is empty");

		T state = loadSnapshot(lastRevision);
		return new StateWithRevision<>(lastRevision, state);
	}

	record StateWithRevision<R, T>(R revision, T state) {
	}
}
