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

import io.activej.bytebuf.ByteBuf;
import io.activej.common.recycle.Recyclable;
import io.activej.common.recycle.Recyclers;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static io.activej.eventloop.util.RunnableWithContext.wrapContext;

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

	public static ChannelConsumer<ByteBuf> outputStreamAsChannelConsumer(Executor executor, OutputStream outputStream) {
		return new AbstractChannelConsumer<ByteBuf>() {
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
						outputStream.close();
					}
				});
			}

			@Override
			protected void onClosed(@NotNull Exception e) {
				executor.execute(() -> {
					try {
						outputStream.close();
					} catch (IOException ignored) {
					}
				});
			}
		};
	}

	public static OutputStream channelConsumerAsOutputStream(Eventloop eventloop, ChannelConsumer<ByteBuf> channelConsumer) {
		return new OutputStream() {
			@Override
			public void write(int b) throws IOException {
				write(new byte[]{(byte) b}, 0, 1);
			}

			@Override
			public void write(@NotNull byte[] b, int off, int len) throws IOException {
				submit(ByteBuf.wrap(b, off, off + len));
			}

			@Override
			public void close() throws IOException {
				submit(null);
			}

			private void submit(ByteBuf buf) throws IOException {
				CompletableFuture<Void> future = eventloop.submit(() -> channelConsumer.accept(buf));
				try {
					future.get();
				} catch (InterruptedException e) {
					eventloop.execute(wrapContext(channelConsumer, channelConsumer::close));
					throw new IOException(e);
				} catch (ExecutionException e) {
					Exception cause = (Exception) e.getCause();
					if (cause instanceof IOException) throw (IOException) cause;
					if (cause instanceof RuntimeException) throw (RuntimeException) cause;
					throw new IOException(cause);
				}
			}
		};
	}
}
