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

import io.activej.csp.queue.ChannelQueue;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.promise.Promise;

@FunctionalInterface
public interface ChannelInput<T> {
	Promise<Void> set(ChannelSupplier<T> input);

	default ChannelConsumer<T> getConsumer() {
		return getConsumer(new ChannelZeroBuffer<>());
	}

	default ChannelConsumer<T> getConsumer(ChannelQueue<T> queue) {
		Promise<Void> extraAcknowledge = set(queue.getSupplier());
		return queue.getConsumer(extraAcknowledge);
	}

}
