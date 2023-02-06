package io.activej.csp.consumer.impl;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.annotation.ExposedInternals;
import io.activej.csp.consumer.AbstractChannelConsumer;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Executor;

@ExposedInternals
public final class OfOutputStream extends AbstractChannelConsumer<ByteBuf> {
	public final Executor executor;
	public final OutputStream outputStream;

	public OfOutputStream(Executor executor, OutputStream outputStream) {
		this.executor = executor;
		this.outputStream = outputStream;
	}

	@Override
	protected Promise<Void> doAccept(@Nullable ByteBuf buf) {
		return Promise.ofBlocking(executor, () -> {
			if (buf != null) {
				try {
					outputStream.write(buf.array(), buf.head(), buf.readRemaining());
				} finally {
					buf.recycle();
				}
			} else {
				outputStream.flush();
				outputStream.close();
			}
		});
	}

	@Override
	protected void onClosed(Exception e) {
		executor.execute(() -> {
			try {
				outputStream.close();
			} catch (IOException ignored) {
			}
		});
	}
}
