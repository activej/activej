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

import java.util.function.Function;

/**
 * A stream transformer that changes each item according to given function.
 */
public final class StreamMapper<I, O> implements StreamTransformer<I, O> {
	private final Function<I, O> function;
	private final Input input;
	private final Output output;

	private StreamMapper(Function<I, O> function) {
		this.function = function;
		this.input = new Input();
		this.output = new Output();
		input.getAcknowledgement()
				.whenException(output::closeEx);
		output.getAcknowledgement()
				.whenResult(input::acknowledge)
				.whenException(input::closeEx);
	}

	public static <I, O> StreamMapper<I, O> create(Function<I, O> function) {
		return new StreamMapper<>(function);
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
			sync();
		}

		@Override
		protected void onEndOfStream() {
			output.sendEndOfStream();
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
		final StreamDataAcceptor<O> dataAcceptor = output.getDataAcceptor();
		if (dataAcceptor != null) {
			final Function<I, O> function = this.function;
			input.resume(item -> dataAcceptor.accept(function.apply(item)));
		} else {
			input.suspend();
		}
	}

}
