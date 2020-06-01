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

import io.activej.datastream.*;

/**
 * A stream transformer that changes each item according to given function.
 */
public final class StreamTransducer<I, O> implements StreamTransformer<I, O> {
	private final Transducer<I, O, Object> transducer;
	private final Input input;
	private final Output output;

	private Object accumulator;

	private StreamTransducer(Transducer<I, O, ?> transducer) {
		//noinspection unchecked
		this.transducer = (Transducer<I, O, Object>) transducer;
		this.input = new Input();
		this.output = new Output();
		input.getAcknowledgement()
				.whenException(output::closeEx);
		output.getEndOfStream()
				.whenResult(input::acknowledge)
				.whenException(input::closeEx);
	}

	public static <I, O> StreamTransducer<I, O> create(Transducer<I, O, ?> transducer) {
		return new StreamTransducer<>(transducer);
	}

	@Override
	public StreamConsumer<I> getInput() {
		return input;
	}

	@Override
	public StreamSupplier<O> getOutput() {
		return output;
	}

	private final class Input extends AbstractStreamConsumer<I> {
		@Override
		protected void onStarted() {
			final StreamDataAcceptor<O> dataAcceptor = transducer.isOneToMany() ? output::send : output.getDataAcceptor();
			accumulator = transducer.onStarted(dataAcceptor);
			sync();
		}

		@Override
		protected void onEndOfStream() {
			final StreamDataAcceptor<O> dataAcceptor = transducer.isOneToMany() ? output::send : output.getDataAcceptor();
			transducer.onEndOfStream(dataAcceptor, accumulator);
			output.sendEndOfStream();
		}

		@Override
		protected void onCleanup() {
			accumulator = null;
		}
	}

	private final class Output extends AbstractStreamSupplier<O> {
		@Override
		protected void onResumed() {
			sync();
		}

		@Override
		protected void onSuspended() {
			sync();
		}
	}

	private void sync() {
		final Transducer<I, O, Object> transducer = this.transducer;
		final StreamDataAcceptor<O> dataAcceptor = transducer.isOneToMany() ? output::send : output.getDataAcceptor();
		final Object accumulator = this.accumulator;
		if (output.isReady()) {
			input.resume(item -> transducer.onItem(dataAcceptor, item, accumulator));
		} else {
			input.suspend();
		}
	}

}
