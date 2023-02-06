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

package io.activej.csp.process.transformer;

import io.activej.csp.consumer.ChannelConsumer;

@FunctionalInterface
public interface ChannelConsumerTransformer<T, R> {
	R transform(ChannelConsumer<T> consumer);

	static <T> ChannelConsumerTransformer<T, ChannelConsumer<T>> identity() {
		return consumer -> consumer;
	}
}
