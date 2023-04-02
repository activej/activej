package io.activej.csp.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.csp.supplier.AbstractChannelSupplier;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.Reactor;

@ExposedInternals
public final class OfAnotherReactor<T> extends AbstractChannelSupplier<T> {
	public final Reactor anotherReactor;
	public final ChannelSupplier<T> anotherReactorSupplier;

	public OfAnotherReactor(Reactor anotherReactor, ChannelSupplier<T> anotherReactorSupplier) {
		this.anotherReactor = anotherReactor;
		this.anotherReactorSupplier = anotherReactorSupplier;
	}

	@Override
	protected Promise<T> doGet() {
		SettablePromise<T> promise = new SettablePromise<>();
		reactor.startExternalTask();
		anotherReactor.execute(() ->
				anotherReactorSupplier.get()
						.subscribe((item, e) -> {
							reactor.execute(() -> promise.set(item, e));
							reactor.completeExternalTask();
						}));
		return promise;
	}

	@Override
	protected void onClosed(Exception e) {
		reactor.startExternalTask();
		anotherReactor.execute(() -> {
			anotherReactorSupplier.closeEx(e);
			reactor.completeExternalTask();
		});
	}
}
