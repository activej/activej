package io.activej.datastream.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.datastream.supplier.AbstractStreamSupplier;

@ExposedInternals
public final class OfValue<T> extends AbstractStreamSupplier<T> {
	private static final Object TAKEN = new Object();

	public T value;

	public OfValue(T value) {
		this.value = value;
	}

	public T getValue() {
		return value;
	}

	public T takeValue() {
		T value = this.value;
		//noinspection unchecked
		this.value = (T) TAKEN;
		return value;
	}

	@Override
	protected void onResumed() {
		if (isReady() && value != TAKEN) {
			send(takeValue());
		}
		if (value == TAKEN) {
			sendEndOfStream();
		}
	}
}
