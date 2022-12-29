package io.activej.reactor;

public abstract class AbstractReactive implements Reactive {
	protected final Reactor reactor;

	public AbstractReactive(Reactor reactor) {this.reactor = reactor;}

	@Override
	public final Reactor getReactor() {
		return reactor;
	}
}
