package io.activej.csp.consumer.impl;

import io.activej.async.process.AsyncCloseable;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.function.ConsumerEx;
import io.activej.csp.consumer.AbstractChannelConsumer;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import static io.activej.common.exception.FatalErrorHandler.handleError;

@ExposedInternals
public final class OfConsumer<T> extends AbstractChannelConsumer<T> {
	public final ConsumerEx<T> consumer;

	public OfConsumer(ConsumerEx<T> consumer, @Nullable AsyncCloseable closeable) {
		super(closeable);
		this.consumer = consumer;
	}

	@Override
	protected Promise<Void> doAccept(T value) {
		if (value != null) {
			try {
				consumer.accept(value);
			} catch (Exception e) {
				handleError(e, consumer);
				return Promise.ofException(e);
			}
			return Promise.complete();
		}
		return Promise.complete();
	}
}
