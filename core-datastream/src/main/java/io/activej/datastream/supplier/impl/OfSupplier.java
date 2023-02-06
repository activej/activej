package io.activej.datastream.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.common.function.SupplierEx;
import io.activej.datastream.supplier.AbstractStreamSupplier;

import static io.activej.common.exception.FatalErrorHandlers.handleError;

@ExposedInternals
public class OfSupplier<T> extends AbstractStreamSupplier<T> {
	public final SupplierEx<T> supplier;

	public OfSupplier(SupplierEx<T> supplier) {this.supplier = supplier;}

	@Override
	protected void onResumed() {
		while (isReady()) {
			T t;
			try {
				t = supplier.get();
			} catch (Exception ex) {
				handleError(ex, supplier);
				closeEx(ex);
				break;
			}
			if (t != null) {
				send(t);
			} else {
				sendEndOfStream();
				break;
			}
		}
	}
}
