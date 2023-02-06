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

import io.activej.datastream.consumer.AbstractStreamConsumer;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.dsl.HasStreamInputs;
import io.activej.datastream.dsl.HasStreamOutput;
import io.activej.datastream.supplier.AbstractStreamSupplier;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.reactor.ImplicitlyReactive;

import java.util.ArrayList;
import java.util.List;

import static io.activej.common.Checks.checkState;
import static io.activej.reactor.Reactive.checkInReactorThread;

/**
 * It is Stream Transformer which unions all input streams and streams it
 * combination to the destination.
 *
 * @param <T> type of output data
 */
public final class StreamUnion<T> extends ImplicitlyReactive implements HasStreamOutput<T>, HasStreamInputs {
	private final List<Input> inputs = new ArrayList<>();
	private final Output output;
	private boolean started;

	private StreamUnion() {
		this.output = new Output();
	}

	public static <T> StreamUnion<T> create() {
		StreamUnion<T> union = new StreamUnion<>();
		union.getReactor().post(union::start);
		return union;
	}

	@Override
	public List<? extends StreamConsumer<?>> getInputs() {
		return inputs;
	}

	@Override
	public StreamSupplier<T> getOutput() {
		return output;
	}

	public StreamConsumer<T> newInput() {
		checkInReactorThread(this);
		checkState(!started, "Cannot add new inputs after StreamUnion has been started");
		Input input = new Input();
		inputs.add(input);
		input.getAcknowledgement()
				.whenException(output::closeEx);
		output.getAcknowledgement()
				.whenResult(input::acknowledge)
				.whenException(input::closeEx);
		return input;
	}

	private void start() {
		started = true;
		sync();
	}

	public final class Input extends AbstractStreamConsumer<T> {
		@Override
		protected void onStarted() {
			sync();
		}

		@Override
		protected void onEndOfStream() {
			sync();
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
		if (!started) return;
		if (inputs.stream().allMatch(Input::isEndOfStream)) {
			output.sendEndOfStream();
			return;
		}
		for (Input input : inputs) {
			input.resume(output.getDataAcceptor());
		}
	}

}
