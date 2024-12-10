package io.activej.state;

import io.activej.common.annotation.ComponentInterface;

import java.io.IOException;

@ComponentInterface
public interface IStateManager<R extends Comparable<R>, T> extends IStateLoader<T, R> {
	boolean hasDiffsSupport();

	R newRevision() throws IOException;

	void saveSnapshot(T state, R revision) throws IOException;

	void saveDiff(T state, R revision, T stateFrom, R revisionFrom) throws IOException;
}
