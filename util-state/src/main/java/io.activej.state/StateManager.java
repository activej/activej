package io.activej.state;

import io.activej.serializer.SerializeException;
import io.activej.serializer.datastream.DeserializeException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

public interface StateManager<T, R extends Comparable<R>> {
	@NotNull R newRevision() throws IOException;

	@Nullable R getLastSnapshotRevision() throws IOException;

	@Nullable R getLastDiffRevision(@NotNull R currentRevision) throws IOException;

	@NotNull T loadSnapshot(@NotNull R revision) throws IOException, DeserializeException;

	@NotNull T loadDiff(@NotNull T state, @NotNull R revisionFrom, @NotNull R revisionTo) throws IOException;

	void saveSnapshot(@NotNull T state, @NotNull R revision) throws IOException, SerializeException;

	void saveDiff(@NotNull T state, @NotNull R revision, @NotNull T stateFrom, @NotNull R revisionFrom) throws IOException;
}
