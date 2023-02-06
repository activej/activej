package io.activej.csp.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.csp.supplier.AbstractChannelSupplier;
import io.activej.promise.Promise;

@ExposedInternals
public final class Empty<T> extends AbstractChannelSupplier<T> {
	@Override
	protected Promise<T> doGet() {
		return Promise.of(null);
	}
}
