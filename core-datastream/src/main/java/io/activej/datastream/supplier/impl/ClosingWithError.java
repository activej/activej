package io.activej.datastream.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.datastream.supplier.AbstractStreamSupplier;

import static io.activej.common.Utils.nullify;

@ExposedInternals
public final class ClosingWithError<T> extends AbstractStreamSupplier<T> {
	public Exception error;

	public ClosingWithError(Exception e) {
		this.error = e;
	}

	@Override
	protected void onInit() {
		error = nullify(error, this::closeEx);
	}
}
