package io.activej.reactor;

import io.activej.common.Checks;

public interface Reactive {
	boolean CHECK_IN_REACTOR_THREAD = Checks.isEnabled(Reactive.class);

	Reactor getReactor();

	default boolean inReactorThread() {
		return getReactor().inReactorThread();
	}

	default void checkInReactorThread() {
		if (CHECK_IN_REACTOR_THREAD) {
			Checks.checkState(inReactorThread(), "Not in reactor thread");
		}
	}
}
