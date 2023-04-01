package io.activej.csp.consumer.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.csp.consumer.AbstractChannelConsumer;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public final class OfAnotherReactor<T> extends AbstractChannelConsumer<T> {
	public final Reactor anotherReactor;
	public final ChannelConsumer<T> anotherReactorConsumer;

	public OfAnotherReactor(Reactor anotherReactor, ChannelConsumer<T> anotherReactorConsumer) {
		this.anotherReactor = anotherReactor;
		this.anotherReactorConsumer = anotherReactorConsumer;
	}

	@Override
	protected Promise<Void> doAccept(@Nullable T value) {
		SettablePromise<Void> promise = new SettablePromise<>();
		reactor.startExternalTask();
		anotherReactor.execute(() ->
				anotherReactorConsumer.accept(value)
						.run((v, e) -> {
							reactor.execute(() -> promise.set(v, e));
							reactor.completeExternalTask();
						}));
		return promise;
	}

	@Override
	protected void onClosed(Exception e) {
		reactor.startExternalTask();
		anotherReactor.execute(() -> {
			anotherReactorConsumer.closeEx(e);
			reactor.completeExternalTask();
		});
	}
}
