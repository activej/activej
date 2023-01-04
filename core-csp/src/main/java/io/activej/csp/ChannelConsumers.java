/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.csp;

import io.activej.async.function.AsyncSupplier;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.recycle.Recyclable;
import io.activej.common.recycle.Recyclers;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

/**
 * Provides additional functionality for managing {@link ChannelConsumer}s.
 */
@SuppressWarnings("WeakerAccess")
public final class ChannelConsumers {

	/**
	 * Passes iterator's values to the {@code output} until it {@code hasNext()},
	 * then returns a promise of {@code null} as a marker of completion.
	 * <p>
	 * If there was an exception while accepting iterator, a promise of
	 * exception will be returned.
	 *
	 * @param output a {@code ChannelConsumer}, which accepts the iterator
	 * @param it     an {@link Iterator} which provides some values
	 * @param <T>    a data type of passed values
	 * @return a promise of {@code null} as a marker of completion
	 */
	public static <T> Promise<Void> acceptAll(ChannelConsumer<T> output, Iterator<? extends T> it) {
		if (!it.hasNext()) return Promise.complete();
		return Promise.ofCallback(cb -> acceptAllImpl(output, it, false, cb));
	}

	public static <T> Promise<Void> acceptAll(ChannelConsumer<T> output, List<? extends T> list) {
		if (list.isEmpty()) return Promise.complete();
		return Promise.ofCallback(cb -> acceptAllImpl(output, list.iterator(), true, cb));
	}

	private static <T> void acceptAllImpl(ChannelConsumer<T> output, Iterator<? extends T> it, boolean ownership, SettablePromise<Void> cb) {
		while (it.hasNext()) {
			Promise<Void> accept = output.accept(it.next());
			if (accept.isResult()) continue;
			accept.run(($, e) -> {
				if (e == null) {
					acceptAllImpl(output, it, ownership, cb);
				} else {
					if (ownership) {
						it.forEachRemaining(Recyclers::recycle);
					} else {
						Recyclers.recycle(it);
					}
					cb.setException(e);
				}
			});
			return;
		}
		cb.set(null);
	}

	public static <T extends Recyclable> ChannelConsumer<T> recycling() {
		return new RecyclingChannelConsumer<>();
	}

	/**
	 * Creates an asynchronous {@link ChannelConsumer<ByteBuf>} out of some {@link OutputStream}.
	 * <p>
	 * I/O operations are executed using a specified {@link Executor}, so that the channel consumer
	 * operations does not block the reactor.
	 * <p>
	 * Passed {@link OutputStream} will be closed once a resulting {@link ChannelConsumer<ByteBuf>} is closed or
	 * in case an error occurs during channel consumer operations.
	 * <p>
	 * <b>This method should be called from within reactor</b>
	 *
	 * @param executor     an executor that will execute blocking I/O
	 * @param outputStream an {@link OutputStream} that is transformed into a {@link ChannelConsumer<ByteBuf>}
	 * @return a {@link ChannelConsumer<ByteBuf>} out ouf an {@link OutputStream}
	 */
	public static ChannelConsumer<ByteBuf> outputStreamAsChannelConsumer(Executor executor, OutputStream outputStream) {
		return new AbstractChannelConsumer<>() {
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
		};
	}

	/**
	 * Creates an {@link OutputStream} out of a {@link ChannelConsumer<ByteBuf>}.
	 * <p>
	 * Asynchronous operations are executed in a context of a specified {@link Reactor}
	 * <p>
	 * Passed {@link ChannelSupplier<ByteBuf>} will be closed once a resulting {@link OutputStream} is closed or
	 * in case an error occurs while reading data.
	 * <p>
	 * <b>{@link OutputStream}'s methods are blocking, so they should not be called from a reactor</b>
	 *
	 * @param reactor         a reactor that will execute asynchronous operations
	 * @param channelConsumer a {@link ChannelSupplier<ByteBuf>} that is transformed to an {@link OutputStream}
	 * @return an {@link OutputStream} out ouf a {@link ChannelSupplier<ByteBuf>}
	 */
	public static OutputStream channelConsumerAsOutputStream(Reactor reactor, ChannelConsumer<ByteBuf> channelConsumer) {
		return new OutputStream() {
			private boolean isClosed;

			@Override
			public void write(int b) throws IOException {
				write(new byte[]{(byte) b}, 0, 1);
			}

			@SuppressWarnings("NullableProblems")
			@Override
			public void write(byte[] b, int off, int len) throws IOException {
				if (isClosed) {
					throw new IOException("Stream Closed");
				}
				submit(reactor,
						() -> channelConsumer.accept(ByteBuf.wrap(b, off, off + len)),
						this);
			}

			@Override
			public void close() throws IOException {
				if (isClosed) return;
				isClosed = true;
				submit(reactor,
						() -> channelConsumer.acceptEndOfStream()
								.whenComplete(channelConsumer::close),
						this);
			}
		};
	}

	static <T> T submit(Reactor reactor, AsyncSupplier<T> supplier, Closeable closeable) throws IOException {
		CompletableFuture<T> future = reactor.submit(supplier::get);
		try {
			return future.get();
		} catch (InterruptedException e) {
			closeable.close();
			Thread.currentThread().interrupt();
			throw new IOException(e);
		} catch (ExecutionException e) {
			closeable.close();
			Throwable cause = e.getCause();
			if (cause instanceof IOException) throw (IOException) cause;
			if (cause instanceof RuntimeException) throw (RuntimeException) cause;
			if (cause instanceof Exception) throw new IOException(cause);
			if (cause instanceof Error) throw (Error) cause;
			throw new RuntimeException(cause);
		}
	}
}
