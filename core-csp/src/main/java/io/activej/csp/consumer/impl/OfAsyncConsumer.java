package io.activej.csp.consumer.impl;

import io.activej.async.function.AsyncConsumer;
import io.activej.async.process.AsyncCloseable;
import io.activej.common.annotation.ExposedInternals;
import io.activej.csp.consumer.AbstractChannelConsumer;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public final class OfAsyncConsumer<T> extends AbstractChannelConsumer<T> {
	public final AsyncConsumer<T> consumer;

	public OfAsyncConsumer(AsyncConsumer<T> consumer, @Nullable AsyncCloseable closeable) {
		super(closeable);
		this.consumer = consumer;
	}

	@Override
	protected Promise<Void> doAccept(T value) {
		if (value != null) {
			return this.consumer.accept(value);
		}
		return Promise.complete();
	}
}
