package io.activej.datastream.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.datastream.consumer.AbstractStreamConsumer;
import io.activej.datastream.supplier.AbstractStreamSupplier;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.promise.Promise;

@ExposedInternals
public final class OfPromise<T> extends AbstractStreamSupplier<T> {
	public final InternalConsumer internalConsumer = new InternalConsumer();

	public Promise<? extends StreamSupplier<T>> promise;

	public class InternalConsumer extends AbstractStreamConsumer<T> {}

	public OfPromise(Promise<? extends StreamSupplier<T>> promise) {
		this.promise = promise;
	}

	@Override
	protected void onInit() {
		promise
				.whenResult(supplier -> {
					this.getEndOfStream()
							.whenException(supplier::closeEx);
					supplier.getEndOfStream()
							.whenResult(this::sendEndOfStream)
							.whenException(this::closeEx);
					supplier.streamTo(internalConsumer);
				})
				.whenException(this::closeEx);
	}

	@Override
	protected void onResumed() {
		internalConsumer.resume(getDataAcceptor());
	}

	@Override
	protected void onSuspended() {
		internalConsumer.suspend();
	}

	@Override
	protected void onAcknowledge() {
		internalConsumer.acknowledge();
	}

	@Override
	protected void onCleanup() {
		promise = null;
	}
}
