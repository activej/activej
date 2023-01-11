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

package io.activej.datastream.stats;

import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.ChannelConsumerTransformer;
import io.activej.csp.dsl.ChannelSupplierTransformer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamConsumerTransformer;
import io.activej.datastream.processor.StreamSupplierTransformer;

public interface StreamStats<T> extends
		StreamSupplierTransformer<T, StreamSupplier<T>>, StreamConsumerTransformer<T, StreamConsumer<T>>,
		ChannelSupplierTransformer<T, ChannelSupplier<T>>, ChannelConsumerTransformer<T, ChannelConsumer<T>> {
	StreamDataAcceptor<T> createDataAcceptor(StreamDataAcceptor<T> actualDataAcceptor);

	void onStarted();

	void onResume();

	void onSuspend();

	void onEndOfStream();

	void onError(Exception e);

	@Override
	default StreamConsumer<T> transform(StreamConsumer<T> consumer) {
		return consumer.transformWith(StreamStatsForwarder.create(this));
	}

	@Override
	default StreamSupplier<T> transform(StreamSupplier<T> supplier) {
		return supplier.transformWith(StreamStatsForwarder.create(this));
	}

	@Override
	default ChannelSupplier<T> transform(ChannelSupplier<T> supplier) {
		return supplier; // TODO
	}

	@Override
	default ChannelConsumer<T> transform(ChannelConsumer<T> consumer) {
		return consumer; // TODO
	}

	static <T> StreamStats_Basic<T> basic() {
		return new StreamStats_Basic<>();
	}

	static <T> StreamStats_Detailed<T> detailed() {
		return new StreamStats_Detailed<>(null);
	}

	static <T> StreamStats_Detailed<T> detailed(StreamStatsSizeCounter<T> sizeCounter) {
		return new StreamStats_Detailed<>(sizeCounter);
	}
}
