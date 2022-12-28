package io.activej.reactor;

import io.activej.reactor.nio.NioReactor;

public abstract class AbstractNioReactive implements NioReactive {
	protected final NioReactor reactor;

	public AbstractNioReactive() {this.reactor = Reactor.getCurrentReactor();}

	public AbstractNioReactive(NioReactor reactor) {this.reactor = reactor;}

	@Override
	public final NioReactor getReactor() {
		return reactor;
	}
}
