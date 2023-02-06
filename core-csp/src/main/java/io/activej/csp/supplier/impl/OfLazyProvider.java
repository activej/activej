package io.activej.csp.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.csp.supplier.AbstractChannelSupplier;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.promise.Promise;

import java.util.function.Supplier;

@ExposedInternals
public final class OfLazyProvider<T> extends AbstractChannelSupplier<T> {
	public final Supplier<? extends ChannelSupplier<T>> provider;

	public ChannelSupplier<T> supplier;

	public OfLazyProvider(Supplier<? extends ChannelSupplier<T>> provider) {
		this.provider = provider;
	}

	@Override
	protected Promise<T> doGet() {
		if (supplier == null) supplier = provider.get();
		return supplier.get();
	}

	@Override
	protected void onClosed(Exception e) {
		if (supplier != null) {
			supplier.closeEx(e);
		}
	}
}
