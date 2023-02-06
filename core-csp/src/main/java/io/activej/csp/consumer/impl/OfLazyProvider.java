package io.activej.csp.consumer.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.csp.consumer.AbstractChannelConsumer;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

@ExposedInternals
public final class OfLazyProvider<T> extends AbstractChannelConsumer<T> {
	public final Supplier<? extends ChannelConsumer<T>> provider;

	public @Nullable ChannelConsumer<T> consumer;

	public OfLazyProvider(Supplier<? extends ChannelConsumer<T>> provider) {
		this.provider = provider;
	}

	@Override
	protected Promise<Void> doAccept(@Nullable T value) {
		if (consumer == null) consumer = provider.get();
		return consumer.accept(value);
	}

	@Override
	protected void onClosed(Exception e) {
		if (consumer != null) {
			consumer.closeEx(e);
		}
	}
}
