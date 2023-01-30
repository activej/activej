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

public final class StreamLimiter<T> extends ImplicitlyReactive implements StreamTransformer<T, T> {
	public static final long NO_LIMIT = -1;

	private final Input input;
	private final Output output;

	public StreamLimiter(long limit) {
		this.input = new Input(limit);
		this.output = new Output();

		input.getAcknowledgement()
				.whenException(output::closeEx);
		output.getAcknowledgement()
				.whenResult(input::acknowledge)
				.whenException(input::closeEx);
	}

	public static <T> StreamLimiter<T> create(long limit) {
		checkState(limit >= NO_LIMIT, "Limit cannot be a negative value");

		return new StreamLimiter<>(limit);
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
		private long limit;

		private Input(long limit) {
			this.limit = limit;
		}

		@Override
		protected void onStarted() {
			if (limit == 0) {
				acknowledge();
				output.sendEndOfStream();
				return;
			}
			sync();
		}

		@Override
		protected void onEndOfStream() {
			output.sendEndOfStream();
		}
	}

	public final class Output extends AbstractStreamSupplier<T> {
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
		if (input.limit == 0) return;

		StreamDataAcceptor<T> dataAcceptor = output.getDataAcceptor();
		if (dataAcceptor != null) {
			input.resume(item -> {
				dataAcceptor.accept(item);

				if (input.limit != NO_LIMIT && --input.limit == 0) {
					input.suspend();
					output.sendEndOfStream();
				}
			});
		} else {
			input.suspend();
		}
	}

}
