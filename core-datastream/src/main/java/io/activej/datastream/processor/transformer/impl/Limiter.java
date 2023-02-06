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

import io.activej.common.annotation.ExposedInternals;
import io.activej.datastream.consumer.AbstractStreamConsumer;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.processor.transformer.StreamTransformer;
import io.activej.datastream.supplier.AbstractStreamSupplier;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.reactor.ImplicitlyReactive;

@ExposedInternals
public final class Limiter<T> extends ImplicitlyReactive implements StreamTransformer<T, T> {
	public static final long NO_LIMIT = -1;

	public final Input input;
	public final Output output;

	public Limiter(long limit) {
		this.input = new Input(limit);
		this.output = new Output();

		input.getAcknowledgement()
				.whenException(output::closeEx);
		output.getAcknowledgement()
				.whenResult(input::acknowledge)
				.whenException(input::closeEx);
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
		public long limit;

		public Input(long limit) {
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
