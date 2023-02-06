package io.activej.datastream.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.datastream.supplier.AbstractStreamSupplier;

import java.util.Iterator;

@ExposedInternals
public final class OfIterator<T> extends AbstractStreamSupplier<T> {
	public final Iterator<T> iterator;

	/**
	 * Creates a new instance of  StreamSupplierOfIterator
	 *
	 * @param iterator iterator with object which need to send
	 */
	public OfIterator(Iterator<T> iterator) {
		this.iterator = iterator;
	}

	@Override
	protected void onResumed() {
		while (isReady() && iterator.hasNext()) {
			send(iterator.next());
		}
		if (!iterator.hasNext()) {
			sendEndOfStream();
		}
	}
}
