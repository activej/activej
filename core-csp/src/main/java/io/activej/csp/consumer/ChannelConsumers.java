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

package io.activej.csp.consumer;

import io.activej.async.function.AsyncConsumer;
import io.activej.async.process.AsyncCloseable;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.annotation.StaticFactories;
import io.activej.common.function.ConsumerEx;
import io.activej.common.recycle.Recyclable;
import io.activej.csp.consumer.impl.*;
import io.activej.csp.queue.ChannelQueue;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

import java.io.OutputStream;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static io.activej.reactor.Reactor.getCurrentReactor;

/**
 * Provides static factories for instantiating various {@link ChannelConsumer}s.
 */
@SuppressWarnings("WeakerAccess")
@StaticFactories(ChannelConsumer.class)
public class ChannelConsumers {

	/**
	 * Wraps {@link AsyncConsumer} in {@link ChannelConsumer}.
	 *
	 * @see ChannelConsumers#ofAsyncConsumer(AsyncConsumer, AsyncCloseable)
	 */
	public static <T> ChannelConsumer<T> ofAsyncConsumer(AsyncConsumer<T> consumer) {
		return ofAsyncConsumer(consumer, AsyncCloseable.of(e -> {}));
	}

	/**
	 * Wraps {@link AsyncConsumer} in {@link ChannelConsumer}.
	 *
	 * @param consumer  {@link AsyncConsumer} to be wrapped
	 * @param closeable a {@link AsyncCloseable}, which will be set to the returned {@link ChannelConsumer}
	 * @param <T>       type of data to be consumed
	 * @return {@link ChannelConsumer} which wraps {@link AsyncConsumer}
	 */
	public static <T> ChannelConsumer<T> ofAsyncConsumer(AsyncConsumer<T> consumer, @Nullable AsyncCloseable closeable) {
		return new OfAsyncConsumer<>(closeable, consumer);
	}

	/**
	 * Wraps a {@link ConsumerEx} in {@link ChannelConsumer}.
	 */
	public static <T> ChannelConsumer<T> ofConsumer(ConsumerEx<T> consumer) {
		return ofAsyncConsumer(AsyncConsumer.of(consumer));
	}

	/**
	 * Creates a consumer which always returns a {@link Promise}
	 * of exception when accepting values.
	 *
	 * @param e   an exception which is wrapped in returned
	 *            {@link Promise} when {@code accept()} is called
	 * @param <T> type of data to be consumed
	 * @return a {@link ChannelConsumer} which always
	 * returns a {@link Promise} of exception when accepts values
	 */
	public static <T> ChannelConsumer<T> ofException(Exception e) {
		return new OfException<>(e);
	}

	/**
	 * @see #ofSupplier(AsyncConsumer, ChannelQueue)
	 */
	public static <T> ChannelConsumer<T> ofSupplier(AsyncConsumer<ChannelSupplier<T>> supplierConsumer) {
		return ofSupplier(supplierConsumer, new ChannelZeroBuffer<>());
	}

	public static <T> ChannelConsumer<T> ofSupplier(AsyncConsumer<ChannelSupplier<T>> supplierConsumer, ChannelQueue<T> queue) {
		Promise<Void> extraAcknowledge = supplierConsumer.accept(queue.getSupplier());
		ChannelConsumer<T> result = queue.getConsumer();
		if (extraAcknowledge == Promise.complete()) return result;
		return result
				.withAcknowledgement(ack -> ack.both(extraAcknowledge));
	}

	/**
	 * Unwraps {@link ChannelConsumer} of provided {@link Promise}.
	 * If provided Promise is already successfully completed, its
	 * result will be returned, otherwise an {@link  ChannelConsumer}
	 * is created, which waits for the Promise to be completed before accepting
	 * any value. A {@link Promise} of Exception will be returned if {@link Promise} was completed
	 * with an exception.
	 *
	 * @param promise {@link Promise} of {@link  ChannelConsumer}
	 * @param <T>     type of data to be consumed
	 * @return {@link ChannelConsumer} of a given promise
	 */
	public static <T> ChannelConsumer<T> ofPromise(Promise<? extends ChannelConsumer<T>> promise) {
		if (promise.isResult()) return promise.getResult();
		return new OfPromise<>(promise);
	}

	public static <T> ChannelConsumer<T> ofAnotherReactor(Reactor anotherReactor, ChannelConsumer<T> anotherReactorConsumer) {
		if (getCurrentReactor() == anotherReactor) {
			return anotherReactorConsumer;
		}
		return new OfAnotherReactor<>(anotherReactor, anotherReactorConsumer);
	}

	/**
	 * Returns a {@link ChannelConsumer} wrapped in {@link Supplier}
	 * and calls its {@code accept()} when {@code accept()} method is called.
	 *
	 * @param provider provider of the {@code ChannelConsumer}
	 * @return a {@code ChannelConsumer} which was wrapped in the {@code provider}
	 */
	public static <T> ChannelConsumer<T> ofLazyProvider(Supplier<? extends ChannelConsumer<T>> provider) {
		return new OfLazyProvider<>(provider);
	}

	/**
	 * Wraps {@link ITcpSocket#write(ByteBuf)} operation into {@link ChannelConsumer}.
	 *
	 * @return {@link ChannelConsumer} of ByteBufs that will be sent to network
	 */
	public static ChannelConsumer<ByteBuf> ofSocket(ITcpSocket socket) {
		return ChannelConsumers.ofAsyncConsumer(socket::write, socket)
				.withAcknowledgement(ack -> ack
						.then(() -> socket.write(null)));
	}

	public static <T extends Recyclable> ChannelConsumer<T> recycling() {
		return new Recycling<>();
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
	public static ChannelConsumer<ByteBuf> ofOutputStream(Executor executor, OutputStream outputStream) {
		return new OfOutputStream(executor, outputStream);
	}
}
