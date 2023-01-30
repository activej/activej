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
import io.activej.reactor.ImplicitlyReactive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.activej.common.Checks.checkState;
import static io.activej.reactor.Reactive.checkInReactorThread;

/**
 * It is Stream Transformer which divides input stream  into groups with some key
 * function, and sends obtained streams to consumers.
 *
 * @param <I> type of input items
 * @param <O> type of output items
 */
@SuppressWarnings("unchecked")
public final class StreamSplitter<I, O> extends ImplicitlyReactive implements HasStreamInput<I>, HasStreamOutputs<O> {
	private final Function<StreamDataAcceptor<O>[], StreamDataAcceptor<I>> acceptorFactory;
	private final Input input;
	private final List<Output> outputs = new ArrayList<>();

	private StreamDataAcceptor<O>[] dataAcceptors = new StreamDataAcceptor[8];

	private boolean started;
	private int completed;

	private StreamSplitter(Function<StreamDataAcceptor<O>[], StreamDataAcceptor<I>> acceptorFactory) {
		this.acceptorFactory = acceptorFactory;
		this.input = new Input();
	}

	public static <I, O> StreamSplitter<I, O> create(BiConsumer<I, StreamDataAcceptor<O>[]> action) {
		return create(acceptors -> item -> action.accept(item, acceptors));
	}

	public static <I, O> StreamSplitter<I, O> create(Function<StreamDataAcceptor<O>[], StreamDataAcceptor<I>> acceptorFactory) {
		StreamSplitter<I, O> streamSplitter = new StreamSplitter<>(acceptorFactory);
		streamSplitter.getReactor().post(streamSplitter::start);
		return streamSplitter;
	}

	public StreamSupplier<O> newOutput() {
		checkInReactorThread(this);
		return addOutput(new Output());
	}

	public StreamSupplier<O> addOutput(Output output) {
		checkState(!started, "Cannot add new outputs after StreamSplitter has been started");
		checkState(output.index == outputs.size(), "Invalid index");
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
		if (outputs.isEmpty()) {
			input.acknowledge();
			return;
		}
		started = true;
		dataAcceptors = Arrays.copyOf(dataAcceptors, outputs.size());
		sync();
	}

	public final class Input extends AbstractStreamConsumer<I> {
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

		@Override
		protected void onError(Exception e) {
			outputs.forEach(output -> output.closeEx(e));
		}
	}

	public class Output extends AbstractStreamSupplier<O> {
		final int index = outputs.size();

		@Override
		protected void onResumed() {
			dataAcceptors[index] = getDataAcceptor();
			sync();
		}

		@Override
		protected void onSuspended() {
			dataAcceptors[index] = getBufferedDataAcceptor();
			sync();
		}

		@Override
		protected void onError(Exception e) {
			input.closeEx(e);
		}

		@Override
		protected void onAcknowledge() {
			complete();
		}

		protected final void sync() {
			StreamSplitter.this.sync();
		}

		protected final void complete() {
			if (++completed == dataAcceptors.length) {
				input.acknowledge();
			}
		}

		protected boolean canProceed() {
			return isReady();
		}
	}

	private void sync() {
		if (!started) return;
		if (outputs.stream().allMatch(Output::canProceed)) {
			input.resume(acceptorFactory.apply(dataAcceptors));
		} else {
			input.suspend();
		}
	}

}
