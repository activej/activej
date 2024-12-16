package io.activej.state;

import io.activej.common.annotation.ComponentInterface;

import java.io.IOException;

@ComponentInterface
public interface IStateManager<R extends Comparable<R>, T> extends IStateLoader<R, T> {
	R newRevision() throws IOException;

	R save(T state) throws IOException;

	void save(T state, R revision) throws IOException;

	void saveSnapshot(T state, R revision) throws IOException;

	void saveDiff(T state, R revision, T stateFrom, R revisionFrom) throws IOException;
}
