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

import io.activej.csp.consumer.ChannelConsumer;
import io.activej.csp.queue.ChannelQueue;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.promise.Promise;

@FunctionalInterface
public interface ChannelOutput<T> {
	void set(ChannelConsumer<T> output);

	default ChannelSupplier<T> getSupplier() {
		return getSupplier(new ChannelZeroBuffer<>());
	}

	default ChannelSupplier<T> getSupplier(ChannelQueue<T> queue) {
		set(queue.getConsumer());
		return queue.getSupplier();
	}

	@SuppressWarnings("UnusedReturnValue")
	default Promise<Void> bindTo(ChannelInput<T> to) {
		return bindTo(to, new ChannelZeroBuffer<>());
	}

	default Promise<Void> bindTo(ChannelInput<T> to, ChannelQueue<T> queue) {
		Promise<Void> extraAcknowledgement = to.set(queue.getSupplier());
		set(queue.getConsumer(extraAcknowledgement));
		return extraAcknowledgement;
	}

}
