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

package io.activej.datastream.processor;

import io.activej.datastream.StreamDataAcceptor;

public final class StreamCounter<T> extends StreamFilter<T, T> {
	private CounterDataAcceptor acceptor = new CounterDataAcceptor(null, 0L);

	private StreamCounter() {}

	public static <T> StreamCounter<T> create() {
		return new StreamCounter<>();
	}

	@Override
	protected StreamDataAcceptor<T> onResumed(StreamDataAcceptor<T> output) {
		acceptor = new CounterDataAcceptor(output, acceptor.itemCount);
		return acceptor;
	}

	public long getItemCount() {
		return acceptor.itemCount;
	}

	private class CounterDataAcceptor implements StreamDataAcceptor<T> {
		private final StreamDataAcceptor<T> output;
		private long itemCount;

		public CounterDataAcceptor(StreamDataAcceptor<T> output, long itemCount) {
			this.output = output;
			this.itemCount = itemCount;
		}

		@Override
		public void accept(T item) {
			itemCount++;
			output.accept(item);
		}
	}
}
