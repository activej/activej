package io.activej.csp.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.common.recycle.Recyclers;
import io.activej.csp.supplier.AbstractChannelSupplier;
import io.activej.promise.Promise;

import static io.activej.common.Utils.nullify;

@ExposedInternals
public final class OfValue<T> extends AbstractChannelSupplier<T> {
	public T item;

	public OfValue(T item) {
		this.item = item;
	}

	public T getValue() {
		return item;
	}

	public T takeValue() {
		T item = this.item;
		this.item = null;
		return item;
	}

	@Override
	protected Promise<T> doGet() {
		T item = takeValue();
		return Promise.of(item);
	}

	@Override
	protected void onCleanup() {
		item = nullify(item, Recyclers::recycle);
	}
}
