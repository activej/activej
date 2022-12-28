package io.activej.reactor;

import static io.activej.reactor.Reactor.getCurrentReactor;

public abstract class AbstractReactive implements Reactive {
	protected final Reactor reactor;

	public AbstractReactive() {this.reactor = getCurrentReactor();}

	public AbstractReactive(Reactor reactor) {this.reactor = reactor;}

	@Override
	public final Reactor getReactor() {
		return reactor;
	}
}
