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

import io.activej.csp.dsl.ChannelConsumerTransformer;
import io.activej.csp.queue.ChannelQueue;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.promise.Promise;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

@FunctionalInterface
public interface ChannelOutput<T> {
	void set(ChannelConsumer<T> output);

	default void set(Promise<ChannelConsumer<T>> outputPromise) {
		set(ChannelConsumer.ofPromise(outputPromise));
	}

	default ChannelSupplier<T> getSupplier() {
		return getSupplier(new ChannelZeroBuffer<>());
	}

	default ChannelSupplier<T> getSupplier(ChannelQueue<T> queue) {
		set(queue.getConsumer());
		return queue.getSupplier();
	}

	default <R> ChannelOutput<R> transformWith(ChannelConsumerTransformer<R, ChannelConsumer<T>> fn) {
		return output -> set(output.transformWith(fn));
	}

	default <R> ChannelOutput<R> map(Function<? super T, ? extends R> fn) {
		return output -> set(output.map(fn));
	}

	default <R> ChannelOutput<R> mapAsync(Function<? super T, ? extends Promise<R>> fn) {
		return output -> set(output.mapAsync(fn));
	}

	default ChannelOutput<T> filter(Predicate<? super T> predicate) {
		return output -> set(output.filter(predicate));
	}

	default ChannelOutput<T> peek(Consumer<? super T> peek) {
		return output -> set(output.peek(peek));
	}

	default Promise<Void> bindTo(ChannelInput<T> to) {
		return bindTo(to, new ChannelZeroBuffer<>());
	}

	default Promise<Void> bindTo(ChannelInput<T> to, ChannelQueue<T> queue) {
		Promise<Void> extraAcknowledgement = to.set(queue.getSupplier());
		set(queue.getConsumer(extraAcknowledgement));
		return extraAcknowledgement;
	}

}
