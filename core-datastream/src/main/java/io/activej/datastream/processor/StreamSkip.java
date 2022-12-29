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
import io.activej.reactor.ImplicitlyReactive;

import static io.activej.common.Checks.checkState;

public final class StreamSkip<T> extends ImplicitlyReactive implements StreamTransformer<T, T> {
	public static final long NO_SKIP = 0;

	private final Input input;
	private final Output output;

	private StreamSkip(long skip) {
		this.input = new Input(skip);
		this.output = new Output();

		input.getAcknowledgement()
				.whenException(output::closeEx);
		output.getAcknowledgement()
				.whenResult(input::acknowledge)
				.whenException(input::closeEx);
	}

	public static <T> StreamSkip<T> create(long skip) {
		checkState(skip >= NO_SKIP, "Skip value cannot be a negative value");

		return new StreamSkip<>(skip);
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
		private long skip;

		private Input(long skip) {
			this.skip = skip;
		}

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
		StreamDataAcceptor<T> dataAcceptor = output.getDataAcceptor();
		if (dataAcceptor != null) {
			input.resume(item -> {
				if (input.skip == NO_SKIP) {
					dataAcceptor.accept(item);
					return;
				}

				input.skip--;
			});
		} else {
			input.suspend();
		}
	}

}
