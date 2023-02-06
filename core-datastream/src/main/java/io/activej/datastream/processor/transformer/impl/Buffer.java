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

package io.activej.datastream.processor.transformer.impl;

import io.activej.common.ApplicationSettings;
import io.activej.common.Checks;
import io.activej.common.annotation.ExposedInternals;
import io.activej.datastream.consumer.AbstractStreamConsumer;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.processor.transformer.StreamTransformer;
import io.activej.datastream.supplier.AbstractStreamSupplier;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.reactor.ImplicitlyReactive;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static java.lang.Integer.numberOfLeadingZeros;

@ExposedInternals
public final class Buffer<T> extends ImplicitlyReactive implements StreamTransformer<T, T> {
	public static final boolean CHECKS = Checks.isEnabled(Buffer.class);
	public static final boolean NULLIFY_ON_TAKE_OUT = ApplicationSettings.getBoolean(Buffer.class, "nullifyOnTakeOut", true);

	public final Input input;
	public final Output output;

	public final Object[] elements;
	public int tail;
	public int head;

	public final int bufferMinSize;
	public final int bufferMaxSize;

	public final StreamDataAcceptor<T> toBuffer;

	public Buffer(int bufferMinSize, int bufferMaxSize) {
		checkArgument(bufferMaxSize > 0 && bufferMinSize >= 0);
		this.bufferMinSize = bufferMinSize;
		this.bufferMaxSize = bufferMaxSize;
		this.elements = new Object[1 << (32 - numberOfLeadingZeros(this.bufferMaxSize - 1))];
		this.input = new Input();
		this.output = new Output();
		this.toBuffer = item -> {
			doAdd(item);
			if (size() >= bufferMaxSize) {
				input.suspend();
				output.flush();
			}
		};
		input.getAcknowledgement()
				.whenException(output::closeEx);
		output.getAcknowledgement()
				.whenResult(input::acknowledge)
				.whenException(input::closeEx);
	}

	public boolean isSaturated() {
		return size() >= bufferMaxSize;
	}

	public boolean isExhausted() {
		return size() <= bufferMinSize;
	}

	public boolean isEmpty() {
		return tail == head;
	}

	public int size() {
		return tail - head;
	}

	private void doAdd(T value) {
		elements[(tail++) & (elements.length - 1)] = value;
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamSupplier<T> getOutput() {
		return output;
	}

	public final class Input extends AbstractStreamConsumer<T> {
		@Override
		protected void onStarted() {
			sync();
		}

		@Override
		protected void onEndOfStream() {
			output.flush();
		}
	}

	public final class Output extends AbstractStreamSupplier<T> {
		@Override
		protected void onResumed() {
			flush();
		}

		void flush() {
			int head = Buffer.this.head;
			int tail = Buffer.this.tail;
			StreamDataAcceptor<T> acceptor;
			while (true) {
				acceptor = getDataAcceptor();
				if (acceptor == null) break;
				if (head == tail) break;
				int pos = (head++) & (elements.length - 1);
				//noinspection unchecked
				T item = (T) elements[pos];
				if (NULLIFY_ON_TAKE_OUT) {
					elements[pos] = null;
				}
				acceptor.accept(item);
			}
			if (CHECKS) checkState(tail == Buffer.this.tail, "New items have been added to buffer while flushing");
			Buffer.this.head = head;
			if (isEmpty() && input.isEndOfStream()) {
				sendEndOfStream();
			}
			sync();
		}

		@Override
		protected void onSuspended() {
			sync();
		}
	}

	private void sync() {
		if (size() >= bufferMaxSize) {
			input.suspend();
		} else if (size() <= bufferMinSize) {
			if (isEmpty() && output.isReady()) {
				input.resume(output.getDataAcceptor());
			} else {
				input.resume(toBuffer);
			}
		}
	}

}
