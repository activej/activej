package io.activej.reactor;

import io.activej.common.Checks;

public interface Reactive {
	Reactor getReactor();

	default void checkInReactorThread() {
		Checks.checkState(getReactor().inReactorThread(), "Not in reactor thread");
	}
}
