package io.activej.datastream.consumer.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.datastream.consumer.AbstractStreamConsumer;
import io.activej.promise.Promise;

@ExposedInternals
public final class OfChannelConsumer<T> extends AbstractStreamConsumer<T> {
	public final ChannelConsumer<T> consumer;
	public boolean working;

	public OfChannelConsumer(ChannelConsumer<T> consumer) {
		this.consumer = consumer;
	}

	@Override
	protected void onStarted() {
		flush();
	}

	private void flush() {
		resume(item -> {
			Promise<Void> promise = consumer.accept(item)
					.whenException(this::closeEx);
			if (promise.isComplete()) return;
			suspend();
			working = true;
			promise.whenResult(() -> {
				working = false;
				if (!isEndOfStream()) {
					flush();
				} else {
					sendEndOfStream();
				}
			});
		});
	}

	@Override
	protected void onEndOfStream() {
		// end of stream is sent either from here or from queues waiting put promise
		// callback, but not from both and this condition ensures that
		if (!working) {
			sendEndOfStream();
		}
	}

	private void sendEndOfStream() {
		consumer.acceptEndOfStream()
				.whenResult(this::acknowledge)
				.whenException(this::closeEx);
	}

	@Override
	protected void onError(Exception e) {
		consumer.closeEx(e);
	}
}
