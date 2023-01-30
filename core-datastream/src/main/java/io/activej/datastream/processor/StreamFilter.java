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

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Provides you apply function before sending data to the destination. It is a {@link StreamFilter}
 * which receives specified type and streams set of function's result  to the destination .
 */
public abstract class StreamFilter<I, O> extends ImplicitlyReactive implements StreamTransformer<I, O> {
	private final Input input;
	private final Output output;

	protected StreamFilter() {
		this.input = new Input();
		this.output = new Output();

		input.getAcknowledgement()
				.whenException(output::closeEx);
		output.getAcknowledgement()
				.whenResult(input::acknowledge)
				.whenException(input::closeEx);
	}

	public static <T> StreamFilter<T, T> create(Predicate<T> predicate) {
		return new StreamFilter<>() {
			@Override
			protected StreamDataAcceptor<T> onResumed(StreamDataAcceptor<T> output) {
				return item -> {if (predicate.test(item)) output.accept(item);};
			}
		};
	}

	public static <I, O> StreamFilter<I, O> mapper(Function<I, O> function) {
		return new StreamFilter<>() {
			@Override
			protected StreamDataAcceptor<I> onResumed(StreamDataAcceptor<O> output) {
				return item -> output.accept(function.apply(item));
			}
		};
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
			StreamFilter.this.onStarted(output::send);
			sync();
		}

		@Override
		protected void onEndOfStream() {
			StreamFilter.this.onEndOfStream(output::send);
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
