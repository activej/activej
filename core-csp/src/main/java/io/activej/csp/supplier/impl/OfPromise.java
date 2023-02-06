package io.activej.csp.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.csp.supplier.AbstractChannelSupplier;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.promise.Promise;

@ExposedInternals
public final class OfPromise<T> extends AbstractChannelSupplier<T> {
	public final Promise<? extends ChannelSupplier<T>> promise;

	public ChannelSupplier<T> supplier;

	public OfPromise(Promise<? extends ChannelSupplier<T>> promise) {
		this.promise = promise;
	}

	@Override
	protected Promise<T> doGet() {
		if (supplier != null) return supplier.get();
		return promise.then(supplier -> {
			this.supplier = supplier;
			return supplier.get();
		});
	}

	@Override
	protected void onClosed(Exception e) {
		promise.whenResult(supplier -> supplier.closeEx(e));
	}
}
