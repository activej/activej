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

package io.activej.datastream.consumer;

import io.activej.async.function.AsyncConsumer;
import io.activej.common.annotation.StaticFactories;
import io.activej.common.function.ConsumerEx;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.datastream.consumer.impl.*;
import io.activej.datastream.processor.transformer.StreamTransformer;
import io.activej.datastream.processor.transformer.StreamTransformers;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;

import java.util.function.Consumer;

@StaticFactories(StreamConsumer.class)
public class StreamConsumers {
	/**
	 * Creates a consumer which does not consume anything.
	 * Its acknowledgement completes when the supplier closes.
	 */
	public static <T> StreamConsumer<T> idle() {
		return new Idle<>();
	}

	/**
	 * Creates a consumer which consumes and ignores everything.
	 * Its acknowledgement completes when the supplier closes.
	 */
	public static <T> StreamConsumer<T> skip() {
		return new Skip<>();
	}

	/**
	 * Creates a consumer which calls the provided {@link Consumer} with items
	 * it receives.
	 * Its acknowledgement completes when the supplier closes.
	 */
	public static <T> StreamConsumer<T> ofConsumer(ConsumerEx<T> consumer) {
		return new OfConsumer<>(consumer);
	}

	/**
	 * Creates a consumer that is in the closed state with given error set.
	 */
	public static <T> StreamConsumer<T> closingWithError(Exception e) {
		return new ClosingWithError<>(e);
	}

	/**
	 * Creates a consumer that waits until the promise completes
	 * and then consumer items into the resulting consumer.
	 */
	public static <T> StreamConsumer<T> ofPromise(Promise<? extends StreamConsumer<T>> promise) {
		if (promise.isResult()) return promise.getResult();
		return new OfPromise<>(promise);
	}

	/**
	 * Creates a consumer that streams the received items into a given {@link ChannelConsumer channel consumer}
	 */
	public static <T> StreamConsumer<T> ofChannelConsumer(ChannelConsumer<T> consumer) {
		return new OfChannelConsumer<>(consumer);
	}

	/**
	 * Creates a consumer which sends received items through the supplier received in the callback.
	 * Acknowledge of that consumer will not be set until the promise received from the callback invocation completes.
	 */
	public static <T> StreamConsumer<T> ofSupplier(AsyncConsumer<StreamSupplier<T>> supplier) {
		StreamTransformer<T, T> forwarder = StreamTransformers.identity();
		Promise<Void> extraAcknowledge = supplier.accept(forwarder.getOutput());
		StreamConsumer<T> result = forwarder.getInput();
		if (extraAcknowledge == Promise.complete()) return result;
		return result
				.withAcknowledgement(ack -> ack.both(extraAcknowledge));
	}

	public static <T> StreamConsumer<T> ofAnotherReactor(Reactor anotherReactor, StreamConsumer<T> anotherReactorConsumer) {
		if (Reactor.getCurrentReactor() == anotherReactor) {
			return anotherReactorConsumer;
		}
		return new OfAnotherReactor<>(anotherReactor, anotherReactorConsumer);
	}
}
