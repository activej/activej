package io.activej.csp.supplier.impl;

import io.activej.async.process.AsyncCloseable;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.function.SupplierEx;
import io.activej.csp.supplier.AbstractChannelSupplier;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import static io.activej.common.exception.FatalErrorHandler.handleError;

@ExposedInternals
public final class OfSupplier<T> extends AbstractChannelSupplier<T> {
	public final SupplierEx<T> supplier;

	public OfSupplier(SupplierEx<T> supplier, @Nullable AsyncCloseable closeable) {
		super(closeable);
		this.supplier = supplier;
	}

	@Override
	protected Promise<T> doGet() {
		try {
			return Promise.of(supplier.get());
		} catch (Exception e) {
			handleError(e, supplier);
			return Promise.ofException(e);
		}
	}
}
