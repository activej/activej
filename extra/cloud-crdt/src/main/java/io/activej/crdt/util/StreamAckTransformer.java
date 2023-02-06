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

package io.activej.crdt.util;

import io.activej.datastream.consumer.AbstractStreamConsumer;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.processor.transformer.StreamTransformer;
import io.activej.datastream.supplier.AbstractStreamSupplier;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.reactor.ImplicitlyReactive;

import java.util.function.UnaryOperator;

public final class StreamAckTransformer<T> extends ImplicitlyReactive implements StreamTransformer<T, T> {
	private final Input input;
	private final Output output;

	StreamAckTransformer(UnaryOperator<Promise<Void>> ackFn) {
		this.input = new Input();
		this.output = new Output();

		input.getAcknowledgement()
				.whenException(output::closeEx);
		ackFn.apply(output.getAcknowledgement())
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
		@Override
		protected void onStarted() {
			input.resume(output.getDataAcceptor());
		}

		@Override
		protected void onEndOfStream() {
			output.sendEndOfStream();
		}
	}

	public final class Output extends AbstractStreamSupplier<T> {
		@Override
		protected void onResumed() {
			input.resume(output.getDataAcceptor());
		}

		@Override
		protected void onSuspended() {
			input.suspend();
		}
	}
}
