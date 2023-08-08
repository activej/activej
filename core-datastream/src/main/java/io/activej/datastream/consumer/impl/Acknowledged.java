package io.activej.datastream.consumer.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.datastream.consumer.AbstractStreamConsumer;

@ExposedInternals
public final class Acknowledged<T> extends AbstractStreamConsumer<T> {
	@Override
	protected void onStarted() {
		acknowledge();
	}
}
