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
import io.activej.datastream.processor.StreamReducers.Reducer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;

import static io.activej.common.Preconditions.checkArgument;

/**
 * Applies aggregative functions to the elements from input streams.
 * <p>
 * Searches key of item with key function, selects elements with some key, reductions it and streams result sorted by key.
 * <p>
 * Elements from stream to input must be sorted by keys. It is Stream Transformer
 * because it represents few consumers and one supplier.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class AbstractStreamReducer<K, O, A> implements HasStreamInputs, HasStreamOutput<O> {
	public static final int DEFAULT_BUFFER_SIZE = 2000;

	private final List<Input> inputs = new ArrayList<>();
	private final Output output;
	private final StreamDataAcceptor<O> outputSender;

	private int bufferSize = DEFAULT_BUFFER_SIZE;

	@Nullable
	private Input<?> lastInput;
	@Nullable
	private K key = null;
	@Nullable
	private A accumulator;

	private final PriorityQueue<Input> priorityQueue;
	private int streamsAwaiting;
	private int streamsOpen;

	/**
	 * Creates a new instance of AbstractStreamReducer
	 *
	 * @param keyComparator comparator for compare keys
	 */
	public AbstractStreamReducer(@NotNull Comparator<K> keyComparator) {
		this.output = new Output();
		this.outputSender = output::send;
		this.priorityQueue = new PriorityQueue<>(1, (o1, o2) -> {
			int compare = ((Comparator) keyComparator).compare(o1.headKey, o2.headKey);
			if (compare != 0)
				return compare;
			return o1.index - o2.index;
		});
	}

	protected AbstractStreamReducer<K, O, A> withBufferSize(int bufferSize) {
		checkArgument(bufferSize >= 0, "bufferSize must be positive value, got %s", bufferSize);
		this.bufferSize = bufferSize;
		return this;
	}

	protected <I> StreamConsumer<I> newInput(Function<I, K> keyFunction, Reducer<K, I, O, A> reducer) {
		Input<I> input = new Input<I>(inputs.size(), priorityQueue, keyFunction, reducer, bufferSize);
		inputs.add(input);
		streamsAwaiting++;
		streamsOpen++;
		return input;
	}

	@Override
	public List<? extends StreamConsumer<?>> getInputs() {
		return (List) inputs;
	}

	@Override
	public StreamSupplier<O> getOutput() {
		return output;
	}

	private final class Input<I> extends AbstractStreamConsumer<I> implements StreamDataAcceptor<I> {
		private I headItem;
		private K headKey;
		private final int index;
		private final PriorityQueue<Input> priorityQueue;
		private final ArrayDeque<I> deque = new ArrayDeque<>();
		private final int bufferSize;

		private final Function<I, K> keyFunction;
		private final Reducer<K, I, O, A> reducer;

		private Input(int index,
		              PriorityQueue<Input> priorityQueue, Function<I, K> keyFunction, Reducer<K, I, O, A> reducer, int bufferSize) {
			this.index = index;
			this.priorityQueue = priorityQueue;
			this.keyFunction = keyFunction;
			this.reducer = reducer;
			this.bufferSize = bufferSize;
		}

		@Override
		protected void onStarted() {
			resume(this);
		}

		/**
		 * Processes received item. Adds item to deque, if deque size is buffer size or it is last
		 * input begins to reduce streams
		 *
		 * @param item item to receive
		 */
		@Override
		public void accept(I item) {
			if (headItem == null) {
				headItem = item;
				headKey = keyFunction.apply(headItem);
				priorityQueue.offer(this);
				if (--streamsAwaiting == 0) {
					output.reduce();
				}
			} else {
				deque.offer(item);
				if (deque.size() == bufferSize) {
					suspend();
					output.reduce();
				}
			}
		}

		@Override
		protected void onEndOfStream() {
			streamsOpen--;
			if (headItem == null) {
				streamsAwaiting--;
			}
			output.reduce();
			output.getEndOfStream()
					.whenComplete(this::acknowledge)
					.whenException(this::closeEx);
		}

		@Override
		protected void onError(Throwable e) {
			output.closeEx(e);
		}

		@Override
		protected void onCleanup() {
			deque.clear();
		}
	}

	private final class Output extends AbstractStreamSupplier<O> {

		void reduce() {
			resume();
		}

		@Override
		protected void onResumed() {
			while (streamsAwaiting == 0) {
				Input<Object> input = priorityQueue.poll();
				if (input == null)
					break;
				//noinspection PointlessNullCheck intellij doesn't know
				if (key != null && input.headKey.equals(key)) {
					accumulator = input.reducer.onNextItem(outputSender, key, input.headItem, accumulator);
				} else {
					if (lastInput != null) {
						lastInput.reducer.onComplete(outputSender, key, accumulator);
					}
					key = input.headKey;
					accumulator = input.reducer.onFirstItem(outputSender, key, input.headItem);
				}
				input.headItem = input.deque.poll();
				lastInput = input;
				if (input.headItem != null) {
					input.headKey = input.keyFunction.apply(input.headItem);
					priorityQueue.offer(input);
				} else {
					if (!input.isEndOfStream()) {
						streamsAwaiting++;
						break;
					}
				}
			}

			for (Input input : inputs) {
				if (input.deque.size() <= bufferSize / 2) {
					input.resume(input);
				}
			}

			if (streamsOpen == 0 && priorityQueue.isEmpty()) {
				if (lastInput != null) {
					lastInput.reducer.onComplete(outputSender, key, accumulator);
					lastInput = null;
					key = null;
					accumulator = null;
				}
				output.sendEndOfStream();
			}
		}

		@Override
		protected void onError(Throwable e) {
			for (Input input : inputs) {
				input.closeEx(e);
			}
		}

		@Override
		protected void onCleanup() {
			priorityQueue.clear();
		}
	}
}
