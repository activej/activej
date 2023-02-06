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

import io.activej.common.Utils;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.csp.dsl.ChannelConsumerTransformer;
import io.activej.csp.dsl.ChannelSupplierTransformer;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamConsumerTransformer;
import io.activej.datastream.processor.StreamSupplierTransformer;
import io.activej.datastream.stats.IntrusiveLinkedList.Node;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.promise.Promise;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.UnaryOperator;

import static java.lang.System.currentTimeMillis;

public final class StreamRegistry<V> implements Iterable<V> {
	private final IntrusiveLinkedList<Entry<V>> list = new IntrusiveLinkedList<>();

	public static class Entry<T> {
		private final long timestamp;
		private final T operation;

		private Entry(T operation) {
			this.timestamp = currentTimeMillis();
			this.operation = operation;
		}

		@Override
		public String toString() {
			return operation + " " + (currentTimeMillis() - timestamp);
		}
	}

	public static <V> StreamRegistry<V> create() {
		return new StreamRegistry<>();
	}

	public final class RegisterTransformer<T> implements
			ChannelSupplierTransformer<T, ChannelSupplier<T>>,
			ChannelConsumerTransformer<T, ChannelConsumer<T>>,
			StreamSupplierTransformer<T, StreamSupplier<T>>,
			StreamConsumerTransformer<T, StreamConsumer<T>> {
		private final V value;

		private RegisterTransformer(V value) {
			this.value = value;
		}

		@Override
		public StreamConsumer<T> transform(StreamConsumer<T> consumer) {
			return register(consumer, value);
		}

		@Override
		public StreamSupplier<T> transform(StreamSupplier<T> supplier) {
			return register(supplier, value);
		}

		@Override
		public ChannelConsumer<T> transform(ChannelConsumer<T> consumer) {
			return register(consumer, value);
		}

		@Override
		public ChannelSupplier<T> transform(ChannelSupplier<T> supplier) {
			return register(supplier, value);
		}
	}

	public <T> RegisterTransformer<T> register(V value) {
		return new RegisterTransformer<>(value);
	}

	public <T> ChannelSupplier<T> register(ChannelSupplier<T> supplier, V value) {
		return supplier.withEndOfStream(subscribe(value));
	}

	public <T> ChannelConsumer<T> register(ChannelConsumer<T> consumer, V value) {
		return consumer.withAcknowledgement(subscribe(value));
	}

	public <T> StreamConsumer<T> register(StreamConsumer<T> consumer, V value) {
		return consumer.withAcknowledgement(subscribe(value));
	}

	public <T> StreamSupplier<T> register(StreamSupplier<T> supplier, V value) {
		return supplier.withEndOfStream(subscribe(value));
	}

	private UnaryOperator<Promise<Void>> subscribe(V value) {
		Entry<V> entry = new Entry<>(value);
		Node<Entry<V>> node = list.addFirstValue(entry);
		return promise -> promise
				.whenComplete(() -> list.removeNode(node));
	}

	@SuppressWarnings("NullableProblems")
	@Override
	public Iterator<V> iterator() {
		Iterator<Entry<V>> iterator = list.iterator();
		return new Iterator<>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public V next() {
				return iterator.next().operation;
			}
		};
	}

	@JmxAttribute(name = "")
	public String getString() {
		List<Entry<V>> entries = new ArrayList<>();
		list.forEach(entries::add);
		return Utils.toString(entries);
	}

}
