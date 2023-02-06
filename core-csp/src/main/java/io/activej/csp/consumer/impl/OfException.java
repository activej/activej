package io.activej.csp.consumer.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.common.recycle.Recyclers;
import io.activej.csp.consumer.AbstractChannelConsumer;
import io.activej.promise.Promise;

@ExposedInternals
public final class OfException<T> extends AbstractChannelConsumer<T> {
	public final Exception exception;

	public OfException(Exception exception) {
		this.exception = exception;
	}

	@Override
	protected Promise<Void> doAccept(T value) {
		Recyclers.recycle(value);
		return Promise.ofException(exception);
	}
}
