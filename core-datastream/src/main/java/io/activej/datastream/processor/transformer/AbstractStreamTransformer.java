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

package io.activej.datastream.processor.transformer;

import io.activej.datastream.consumer.AbstractStreamConsumer;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.supplier.AbstractStreamSupplier;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.reactor.ImplicitlyReactive;

/**
 * Provides you apply function before sending data to the destination. It is a {@link AbstractStreamTransformer}
 * which receives specified type and streams set of function's result  to the destination .
 */
public abstract class AbstractStreamTransformer<I, O> extends ImplicitlyReactive implements StreamTransformer<I, O> {
	private final Input input;
	private final Output output;

	protected AbstractStreamTransformer() {
		this.input = new Input();
		this.output = new Output();

		input.getAcknowledgement()
				.whenException(output::closeEx);
		output.getAcknowledgement()
				.whenResult(input::acknowledge)
				.whenException(input::closeEx);
	}

	@Override
	public StreamConsumer<I> getInput() {
		return input;
	}

	@Override
	public StreamSupplier<O> getOutput() {
		return output;
	}

	public final class Input extends AbstractStreamConsumer<I> {
		@Override
		protected void onStarted() {
			AbstractStreamTransformer.this.onStarted(output::send);
			sync();
		}

		@Override
		protected void onEndOfStream() {
			AbstractStreamTransformer.this.onEndOfStream(output::send);
			output.sendEndOfStream();
		}
	}

	public final class Output extends AbstractStreamSupplier<O> {
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
		StreamDataAcceptor<O> dataAcceptor = output.getDataAcceptor();
		if (dataAcceptor != null) {
			input.resume(onResumed(isOneToMany() ? output.getBufferedDataAcceptor() : dataAcceptor));
		} else {
			input.suspend();
		}
	}

	protected boolean isOneToMany() {
		return false;
	}

	protected void onStarted(StreamDataAcceptor<O> output) {
	}

	protected void onEndOfStream(StreamDataAcceptor<O> output) {
	}

	protected abstract StreamDataAcceptor<I> onResumed(StreamDataAcceptor<O> output);

}
