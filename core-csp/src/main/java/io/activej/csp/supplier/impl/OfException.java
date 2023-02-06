package io.activej.csp.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.csp.supplier.AbstractChannelSupplier;
import io.activej.promise.Promise;

@ExposedInternals
public final class OfException<T> extends AbstractChannelSupplier<T> {
	public final Exception exception;

	public OfException(Exception exception) {
		this.exception = exception;
	}

	@Override
	protected Promise<T> doGet() {
		return Promise.ofException(exception);
	}
}
