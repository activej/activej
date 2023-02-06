package io.activej.datastream.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.datastream.consumer.AbstractStreamConsumer;
import io.activej.datastream.supplier.AbstractStreamSupplier;
import io.activej.datastream.supplier.StreamSupplier;

@ExposedInternals
public final class Concat<T> extends AbstractStreamSupplier<T> {
	public ChannelSupplier<StreamSupplier<T>> iterator;
	public InternalConsumer internalConsumer = new InternalConsumer();

	public class InternalConsumer extends AbstractStreamConsumer<T> {}

	public Concat(ChannelSupplier<StreamSupplier<T>> iterator) {
		this.iterator = iterator;
	}

	@Override
	protected void onStarted() {
		next();
	}

	private void next() {
		internalConsumer.acknowledge();
		internalConsumer = new InternalConsumer();
		resume();
		iterator.get()
				.whenResult(supplier -> {
					if (supplier != null) {
						supplier.getEndOfStream()
								.whenResult(this::next)
								.whenException(this::closeEx);
						supplier.streamTo(internalConsumer);
					} else {
						sendEndOfStream();
					}
				})
				.whenException(this::closeEx);
	}

	@Override
	protected void onResumed() {
		internalConsumer.resume(getDataAcceptor());
	}

	@Override
	protected void onAcknowledge() {
		internalConsumer.acknowledge();
	}

	@Override
	protected void onSuspended() {
		internalConsumer.suspend();
	}

	@Override
	protected void onError(Exception e) {
		internalConsumer.closeEx(e);
		iterator.closeEx(e);
	}

	@Override
	protected void onCleanup() {
		iterator = null;
	}
}
