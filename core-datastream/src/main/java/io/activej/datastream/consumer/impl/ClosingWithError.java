package io.activej.datastream.consumer.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.datastream.consumer.AbstractStreamConsumer;

import static io.activej.common.Utils.nullify;

@ExposedInternals
public final class ClosingWithError<T> extends AbstractStreamConsumer<T> {
	public Exception error;

	public ClosingWithError(Exception e) {
		this.error = e;
	}

	@Override
	protected void onInit() {
		error = nullify(error, this::closeEx);
	}
}
