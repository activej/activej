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
import io.activej.datastream.dsl.HasStreamInput;
import io.activej.datastream.dsl.HasStreamOutputs;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promises;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.activej.common.Preconditions.checkState;

/**
 * It is Stream Transformer which divides input stream  into groups with some key
 * function, and sends obtained streams to consumers.
 *
 * @param <I> type of input items
 * @param <O> type of output items
 */
@SuppressWarnings("unchecked")
public final class StreamSplitter<I, O> implements HasStreamInput<I>, HasStreamOutputs<O> {
	private final Function<StreamDataAcceptor<O>[], StreamDataAcceptor<I>> acceptorFactory;
	private final Input input;
	private final List<Output> outputs = new ArrayList<>();

	private StreamDataAcceptor<O>[] dataAcceptors = new StreamDataAcceptor[8];

	private boolean started;

	private StreamSplitter(Function<StreamDataAcceptor<O>[], StreamDataAcceptor<I>> acceptorFactory) {
		this.acceptorFactory = acceptorFactory;
		this.input = new Input();
	}

	public static <I, O> StreamSplitter<I, O> create(BiConsumer<I, StreamDataAcceptor<O>[]> action) {
		return create(acceptors -> item -> action.accept(item, acceptors));
	}

	public static <I, O> StreamSplitter<I, O> create(Function<StreamDataAcceptor<O>[], StreamDataAcceptor<I>> acceptorFactory) {
		StreamSplitter<I, O> streamSplitter = new StreamSplitter<>(acceptorFactory);
		Eventloop.getCurrentEventloop().post(streamSplitter::start);
		return streamSplitter;
	}

	public StreamSupplier<O> newOutput() {
		checkState(!started, "Cannot add new inputs after StreamUnion has been started");
		Output output = new Output(outputs.size());
		outputs.add(output);
		if (outputs.size() > dataAcceptors.length) {
			dataAcceptors = Arrays.copyOf(dataAcceptors, dataAcceptors.length * 2);
		}
		return output;
	}

	@Override
	public StreamConsumer<I> getInput() {
		return input;
	}

	@Override
	public List<? extends StreamSupplier<O>> getOutputs() {
		return outputs;
	}

	private void start() {
		started = true;
		dataAcceptors = Arrays.copyOf(dataAcceptors, outputs.size());
		input.getAcknowledgement()
				.whenException(e -> outputs.forEach(output -> output.closeEx(e)));
		Promises.all(outputs.stream().map(Output::getEndOfStream))
				.whenResult(input::acknowledge)
				.whenException(input::closeEx);
		sync();
	}

	private final class Input extends AbstractStreamConsumer<I> {
		@Override
		protected void onStarted() {
			sync();
		}

		@Override
		protected void onEndOfStream() {
			for (Output output : outputs) {
				output.sendEndOfStream();
			}
		}
	}

	private final class Output extends AbstractStreamSupplier<O> {
		final int index;

		public Output(int index) {
			this.index = index;
		}

		@Override
		protected void onResumed() {
			dataAcceptors[index] = getDataAcceptor();
			sync();
		}

		@Override
		protected void onSuspended() {
			dataAcceptors[index] = getDataAcceptor();
			sync();
		}
	}

	private void sync() {
		if (!started) return;
		if (outputs.stream().allMatch(Output::isReady)) {
			input.resume(acceptorFactory.apply(dataAcceptors));
		} else {
			input.suspend();
		}
	}

}
