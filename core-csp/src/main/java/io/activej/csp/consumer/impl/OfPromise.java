package io.activej.csp.consumer.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.common.recycle.Recyclers;
import io.activej.csp.consumer.AbstractChannelConsumer;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public final class OfPromise<T> extends AbstractChannelConsumer<T> {
	public final Promise<? extends ChannelConsumer<T>> promise;

	public @Nullable ChannelConsumer<T> consumer;

	public OfPromise(Promise<? extends ChannelConsumer<T>> promise) {
		this.promise = promise;
	}

	@Override
	protected Promise<Void> doAccept(T value) {
		if (consumer != null) return consumer.accept(value);
		return promise.then(
				consumer -> {
					this.consumer = consumer;
					return consumer.accept(value);
				},
				e -> {
					Recyclers.recycle(value);
					return Promise.ofException(e);
				});
	}

	@Override
	protected void onClosed(Exception e) {
		promise.whenResult(supplier -> supplier.closeEx(e));
	}
}
