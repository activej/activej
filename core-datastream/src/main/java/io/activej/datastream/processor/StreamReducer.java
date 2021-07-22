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
import io.activej.datastream.dsl.HasStreamInputs;
import io.activej.datastream.dsl.HasStreamOutput;
import io.activej.datastream.processor.StreamReducers.Reducer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;

import static io.activej.common.Checks.checkArgument;

/**
 * Applies aggregative functions to the elements from input streams.
 * <p>
 * Searches key of item with key function, selects elements with some key, reductions it and streams result sorted by key.
 * <p>
 * Elements from stream to input must be sorted by keys. It is Stream Transformer
 * because it represents few consumers and one supplier.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class StreamReducer<K, O, A> implements HasStreamInputs, HasStreamOutput<O> {
	public static final int DEFAULT_BUFFER_SIZE = 2000;

	private final List<Input> inputs = new ArrayList<>();
	private final Output output;

	private int bufferSize = DEFAULT_BUFFER_SIZE;

	@Nullable
	private Input<?> lastInput;
	@Nullable
	private K key = null;
	@Nullable
	private A accumulator;

	private final PriorityQueue<Input<?>> priorityQueue;
	private int streamsAwaiting;
	private int streamsOpen;

	protected StreamReducer(@NotNull PriorityQueue<Input<?>> priorityQueue) {
		this.output = new Output();
		this.priorityQueue = priorityQueue;
	}

	public static <K, O, A> StreamReducer<K, O, A> create(Comparator<K> keyComparator) {
		return new StreamReducer<>(new PriorityQueue<>(1, (input1, input2) -> {
			int compare = ((Comparator) keyComparator).compare(input1.headKey, input2.headKey);
			if (compare != 0) return compare;
			return input1.index - input2.index;
		}));
	}

	@SuppressWarnings({"ComparatorCombinators", "Convert2Diamond"})
	public static <K extends Comparable<K>, O, A> StreamReducer<K, O, A> create() {
		return new StreamReducer<K, O, A>(new PriorityQueue<>(1, (input1, input2) -> {
			int compare = input1.headKey.compareTo(input2.headKey);
			if (compare != 0) return compare;
			return input1.index - input2.index;
		}));
	}

	public StreamReducer<K, O, A> withBufferSize(int bufferSize) {
		checkArgument(bufferSize >= 0, "bufferSize must be positive value, got %s", bufferSize);
		this.bufferSize = bufferSize;
		return this;
	}

	public <I> StreamConsumer<I> newInput(Function<I, K> keyFunction, Reducer<K, I, O, A> reducer) {
		return addInput(new SimpleInput(keyFunction, reducer));
	}

	public <I> Input<I> addInput(Input<I> input) {
		inputs.add(input);
		input.await();
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

	public abstract class Input<I> extends AbstractStreamConsumer<I> implements StreamDataAcceptor<I>, Function<I, K>, Reducer<K, I, O, A> {
		private I headItem;
		private K headKey;
		private final int index;
		private final PriorityQueue<Input<?>> priorityQueue;
		private final ArrayDeque<I> deque = new ArrayDeque<>();
		private final int bufferSize;

		public Input() {
			this.index = StreamReducer.this.inputs.size();
			this.priorityQueue = StreamReducer.this.priorityQueue;
			this.bufferSize = StreamReducer.this.bufferSize;
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
				headKey = this.apply(headItem);
				priorityQueue.offer(this);
				if (advance() == 0) {
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
			closeInput();
			if (headItem == null) {
				advance();
			}
			output.reduce();
			output.getAcknowledgement()
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

		protected int await() {
			return ++streamsAwaiting;
		}

		protected int advance() {
			return --streamsAwaiting;
		}

		protected void closeInput(){
			streamsOpen--;
		}

		protected void continueReduce(){
			output.reduce();
		}
	}

	public class SimpleInput<I> extends Input<I> {
		private final Function<I, K> keyFunction;
		private final Reducer<K, I, O, A> reducer;

		public SimpleInput(Function<I, K> keyFunction, Reducer<K, I, O, A> reducer) {
			this.keyFunction = keyFunction;
			this.reducer = reducer;
		}

		@Override
		public K apply(I item) {
			return keyFunction.apply(item);
		}

		@Override
		public A onFirstItem(StreamDataAcceptor<O> stream, K key, I firstValue) {
			return reducer.onFirstItem(stream, key, firstValue);
		}

		@Override
		public A onNextItem(StreamDataAcceptor<O> stream, K key, I nextValue, A accumulator) {
			return reducer.onNextItem(stream, key, nextValue, accumulator);
		}

		@Override
		public void onComplete(StreamDataAcceptor<O> stream, K key, A accumulator) {
			reducer.onComplete(stream, key, accumulator);
		}
	}

	private final class Output extends AbstractStreamSupplier<O> {
		void reduce() {
			resume();
		}

		@Override
		protected void onResumed() {
			while (streamsAwaiting == 0) {
				Input<Object> input = (Input<Object>) priorityQueue.poll();
				if (input == null)
					break;
				if (input.isComplete())
					continue;
				//noinspection PointlessNullCheck intellij doesn't know
				if (key != null && input.headKey.equals(key)) {
					accumulator = input.onNextItem(getBufferedDataAcceptor(), key, input.headItem, accumulator);
				} else {
					if (lastInput != null) {
						lastInput.onComplete(getBufferedDataAcceptor(), key, accumulator);
					}
					key = input.headKey;
					accumulator = input.onFirstItem(getBufferedDataAcceptor(), key, input.headItem);
				}
				input.headItem = input.deque.poll();
				lastInput = input;
				if (input.headItem != null) {
					input.headKey = input.apply(input.headItem);
					priorityQueue.offer(input);
				} else {
					if (!input.isEndOfStream()) {
						input.await();
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
					lastInput.onComplete(getBufferedDataAcceptor(), key, accumulator);
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
