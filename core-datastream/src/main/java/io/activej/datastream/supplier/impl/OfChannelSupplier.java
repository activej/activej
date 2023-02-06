package io.activej.datastream.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.datastream.supplier.AbstractStreamSupplier;

@ExposedInternals
public final class OfChannelSupplier<T> extends AbstractStreamSupplier<T> {
	public final ChannelSupplier<T> supplier;

	public OfChannelSupplier(ChannelSupplier<T> supplier) {
		this.supplier = supplier;
	}

	@Override
	protected void onResumed() {
		asyncBegin();
		supplier.get()
				.run((item, e) -> {
					if (e == null) {
						if (item != null) {
							send(item);
							asyncResume();
						} else {
							sendEndOfStream();
						}
					} else {
						closeEx(e);
					}
				});
	}

	@Override
	protected void onError(Exception e) {
		supplier.closeEx(e);
	}
}
