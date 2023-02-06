package io.activej.datastream.consumer.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.common.function.ConsumerEx;
import io.activej.datastream.consumer.AbstractStreamConsumer;

import static io.activej.common.exception.FatalErrorHandlers.handleError;

@ExposedInternals
public final class OfConsumer<T> extends AbstractStreamConsumer<T> {
	public final ConsumerEx<T> consumer;

	public OfConsumer(ConsumerEx<T> consumer) {
		this.consumer = consumer;
	}

	@Override
	protected void onStarted() {
		resume(item -> {
			try {
				consumer.accept(item);
			} catch (Exception ex) {
				handleError(ex, this);
				closeEx(ex);
			}
		});
	}

	@Override
	protected void onEndOfStream() {
		acknowledge();
	}
}
