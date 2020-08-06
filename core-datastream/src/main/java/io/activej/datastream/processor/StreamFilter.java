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

import java.util.function.Predicate;

/**
 * Provides you apply function before sending data to the destination. It is a {@link StreamFilter}
 * which receives specified type and streams set of function's result  to the destination .
 */
public final class StreamFilter<T> implements StreamTransformer<T, T> {
	private final Predicate<T> predicate;
	private final Input input;
	private final Output output;

	private StreamFilter(Predicate<T> predicate) {
		this.predicate = predicate;
		this.input = new Input();
		this.output = new Output();

		input.getAcknowledgement()
				.whenException(output::closeEx);
		output.getAcknowledgement()
				.whenResult(input::acknowledge)
				.whenException(input::closeEx);
	}

	public static <T> StreamFilter<T> create(Predicate<T> predicate) {
		return new StreamFilter<>(predicate);
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamSupplier<T> getOutput() {
		return output;
	}

	private final class Input extends AbstractStreamConsumer<T> {
		@Override
		protected void onStarted() {
			sync();
		}

		@Override
		protected void onEndOfStream() {
			output.sendEndOfStream();
		}
	}

	private final class Output extends AbstractStreamSupplier<T> {
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
		final StreamDataAcceptor<T> dataAcceptor = output.getDataAcceptor();
		if (dataAcceptor != null) {
			final Predicate<T> predicate = this.predicate;
			input.resume(item -> {
				if (predicate.test(item)) {
					dataAcceptor.accept(item);
				}
			});
		} else {
			input.suspend();
		}
	}

}
