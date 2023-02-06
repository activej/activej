package io.activej.datastream.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.datastream.supplier.AbstractStreamSupplier;

@ExposedInternals
public final class Empty<T> extends AbstractStreamSupplier<T> {
	@Override
	protected void onInit() {
		sendEndOfStream();
	}
}
