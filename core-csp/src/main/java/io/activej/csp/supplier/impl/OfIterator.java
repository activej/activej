package io.activej.csp.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.common.recycle.Recyclers;
import io.activej.csp.supplier.AbstractChannelSupplier;
import io.activej.promise.Promise;

import java.util.Iterator;

@ExposedInternals
public final class OfIterator<T> extends AbstractChannelSupplier<T> {
	public final Iterator<? extends T> iterator;
	public final boolean ownership;

	public OfIterator(Iterator<? extends T> iterator, boolean ownership) {
		this.iterator = iterator;
		this.ownership = ownership;
	}

	@Override
	protected Promise<T> doGet() {
		return Promise.of(iterator.hasNext() ? iterator.next() : null);
	}

	@Override
	protected void onCleanup() {
		if (ownership) {
			iterator.forEachRemaining(Recyclers::recycle);
		} else {
			Recyclers.recycle(iterator);
		}
	}
}
