package io.activej.state;

import io.activej.common.annotation.ComponentInterface;

import java.io.IOException;

@ComponentInterface
public interface IStateManager<R extends Comparable<R>, T> extends IStateLoader<R, T> {
	R save(T state) throws IOException;

	void save(T state, R revision) throws IOException;
}
