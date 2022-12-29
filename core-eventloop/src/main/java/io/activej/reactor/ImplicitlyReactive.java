package io.activej.reactor;

import static io.activej.reactor.Reactor.getCurrentReactor;

public abstract class ImplicitlyReactive extends AbstractReactive {
	public ImplicitlyReactive() {
		super(getCurrentReactor());
	}
}
