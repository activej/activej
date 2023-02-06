package io.activej.csp.supplier.impl;

import io.activej.async.function.AsyncSupplier;
import io.activej.async.process.AsyncCloseable;
import io.activej.common.annotation.ExposedInternals;
import io.activej.csp.supplier.AbstractChannelSupplier;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public final class OfAsyncSupplier<T> extends AbstractChannelSupplier<T> {
	public final AsyncSupplier<T> supplier;

	public OfAsyncSupplier(@Nullable AsyncCloseable closeable, AsyncSupplier<T> supplier) {
		super(closeable);
		this.supplier = supplier;
	}

	@Override
	protected Promise<T> doGet() {
		return supplier.get();
	}
}
