package io.activej.reactor;

public interface Reactive {
	Reactor getReactor();

	static void checkInReactorThread(Reactive reactive) {
		Reactor.checkInReactorThread(reactive.getReactor());
	}
}
