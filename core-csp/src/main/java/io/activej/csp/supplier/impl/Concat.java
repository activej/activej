package io.activej.csp.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.common.recycle.Recyclers;
import io.activej.csp.supplier.AbstractChannelSupplier;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.promise.Promise;

import java.util.Iterator;

@ExposedInternals
public final class Concat<T> extends AbstractChannelSupplier<T> {
	public final Iterator<? extends ChannelSupplier<? extends T>> iterator;
	public final boolean ownership;

	public ChannelSupplier<? extends T> current = ChannelSuppliers.empty();

	public Concat(Iterator<? extends ChannelSupplier<? extends T>> iterator, boolean ownership) {
		this.iterator = iterator;
		this.ownership = ownership;
	}

	@Override
	protected Promise<T> doGet() {
		return current.get()
				.then((value, e) -> {
					if (e == null) {
						if (value != null) {
							return Promise.of(value);
						} else {
							if (iterator.hasNext()) {
								current = iterator.next();
								return get();
							} else {
								return Promise.of(null);
							}
						}
					} else {
						closeEx(e);
						return Promise.ofException(e);
					}
				});
	}

	@Override
	protected void onClosed(Exception e) {
		current.closeEx(e);
		if (ownership) {
			iterator.forEachRemaining(Recyclers::recycle);
		} else {
			Recyclers.recycle(iterator);
		}
	}
}
