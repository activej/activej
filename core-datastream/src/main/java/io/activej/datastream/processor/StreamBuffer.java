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

import io.activej.common.ApplicationSettings;
import io.activej.common.Checks;
import io.activej.datastream.*;
import io.activej.reactor.ImplicitlyReactive;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static java.lang.Integer.numberOfLeadingZeros;

/**
 * A stream transformer that changes each item according to given function.
 */
public final class StreamBuffer<T> extends ImplicitlyReactive implements StreamTransformer<T, T> {
	private static final boolean CHECKS = Checks.isEnabled(StreamBuffer.class);
	private static final boolean NULLIFY_ON_TAKE_OUT = ApplicationSettings.getBoolean(StreamBuffer.class, "nullifyOnTakeOut", true);

	private final Input input;
	private final Output output;

	private final Object[] elements;
	private int tail;
	private int head;

	private final int bufferMinSize;
	private final int bufferMaxSize;

	private final StreamDataAcceptor<T> toBuffer;

	private StreamBuffer(int bufferMinSize, int bufferMaxSize) {
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

	public static <T> StreamBuffer<T> create(int bufferMinSize, int bufferMaxSize) {
		return new StreamBuffer<>(bufferMinSize, bufferMaxSize);
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
			int head = StreamBuffer.this.head;
			int tail = StreamBuffer.this.tail;
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
			if (CHECKS) checkState(tail == StreamBuffer.this.tail, "New items have been added to buffer while flushing");
			StreamBuffer.this.head = head;
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
