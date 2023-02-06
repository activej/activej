package io.activej.csp.supplier.impl;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.annotation.ExposedInternals;
import io.activej.csp.supplier.AbstractChannelSupplier;
import io.activej.promise.Promise;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Executor;

@ExposedInternals
public final class OfInputStream extends AbstractChannelSupplier<ByteBuf> {
	public final Executor executor;
	public final int bufSize;
	public final InputStream inputStream;

	public OfInputStream(Executor executor, int bufSize, InputStream inputStream) {
		this.executor = executor;
		this.bufSize = bufSize;
		this.inputStream = inputStream;
	}

	@Override
	protected Promise<ByteBuf> doGet() {
		return Promise.ofBlocking(executor, () -> {
			ByteBuf buf = ByteBufPool.allocate(bufSize);
			int readBytes;
			try {
				readBytes = inputStream.read(buf.array(), 0, bufSize);
			} catch (IOException e) {
				buf.recycle();
				throw e;
			}
			if (readBytes != -1) {
				buf.moveTail(readBytes);
				return buf;
			} else {
				buf.recycle();
				return null;
			}
		});
	}

	@Override
	protected void onClosed(Exception e) {
		executor.execute(() -> {
			try {
				inputStream.close();
			} catch (IOException ignored) {
			}
		});
	}
}
