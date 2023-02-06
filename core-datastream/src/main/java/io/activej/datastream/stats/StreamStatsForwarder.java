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

package io.activej.datastream.stats;

import io.activej.datastream.consumer.AbstractStreamConsumer;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.processor.transformer.StreamTransformer;
import io.activej.datastream.supplier.AbstractStreamSupplier;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.reactor.ImplicitlyReactive;

public final class StreamStatsForwarder<T> extends ImplicitlyReactive implements StreamTransformer<T, T> {
	private final Input input;
	private final Output output;

	private final StreamStats<T> stats;

	private StreamStatsForwarder(StreamStats<T> stats) {
		this.stats = stats;
		this.input = new Input();
		this.output = new Output();
		input.getAcknowledgement()
				.whenException(output::closeEx);
		output.getAcknowledgement()
				.whenResult(input::acknowledge)
				.whenException(input::closeEx);
	}

	public static <T> StreamStatsForwarder<T> create(StreamStats<T> stats) {
		return new StreamStatsForwarder<>(stats);
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
			stats.onStarted();
			StreamDataAcceptor<T> dataAcceptor = output.getDataAcceptor();
			if (dataAcceptor != null) {
				resume(stats.createDataAcceptor(dataAcceptor));
			}
		}

		@Override
		protected void onEndOfStream() {
			stats.onEndOfStream();
			output.sendEndOfStream();
		}

		@Override
		protected void onError(Exception e) {
			stats.onError(e);
		}
	}

	public final class Output extends AbstractStreamSupplier<T> {
		@Override
		protected void onResumed() {
			stats.onResume();
			input.resume(stats.createDataAcceptor(getDataAcceptor()));
		}

		@Override
		protected void onSuspended() {
			stats.onSuspend();
			input.suspend();
		}
	}
}
