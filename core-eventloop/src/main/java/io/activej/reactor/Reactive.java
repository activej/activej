package io.activej.reactor;

public interface Reactive {
	Reactor getReactor();

	default boolean inReactorThread() {
		return getReactor().inReactorThread();
	}
}
