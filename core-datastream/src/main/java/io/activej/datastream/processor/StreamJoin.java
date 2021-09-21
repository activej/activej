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

import io.activej.common.initializer.WithInitializer;
import io.activej.datastream.*;
import io.activej.datastream.dsl.HasStreamInputs;
import io.activej.datastream.dsl.HasStreamOutput;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.function.Function;

import static java.util.Arrays.asList;

/**
 * Represents an object which has left and right consumers and one supplier. After receiving data
 * it can join it, or either does an inner-join or a left join. It works similar to joins from SQL.
 * It is a {@link StreamJoin} which receives specified type and streams
 * set of join's result to the destination.
 */
public final class StreamJoin<K, L, R, V> implements HasStreamInputs, HasStreamOutput<V>, WithInitializer<StreamJoin<K, L, R, V>> {

	/**
	 * It is the primary interface of a joiner. It contains methods which will join streams
	 */
	public interface Joiner<K, L, R, V> {
		/**
		 * Streams objects with all fields from both received streams as long as there is a match
		 * between the keys in both items.
		 */
		void onInnerJoin(K key, L left, R right, StreamDataAcceptor<V> output);

		/**
		 * Streams objects with all fields from the left stream, with the matching key - fields in the
		 * right stream. The field of result object is NULL in the right stream when there is no match.
		 */
		void onLeftJoin(K key, L left, StreamDataAcceptor<V> output);
	}

	/**
	 * Represents a joiner that produces only inner joins
	 */
	public abstract static class InnerJoiner<K, L, R, V> implements Joiner<K, L, R, V> {
		/**
		 * Left join does nothing for absence null fields in result inner join
		 */
		@Override
		public void onLeftJoin(K key, L left, StreamDataAcceptor<V> output) {
		}
	}

	/**
	 * Simple implementation of Joiner, which does inner and left join
	 */
	public abstract static class ValueJoiner<K, L, R, V> implements Joiner<K, L, R, V> {
		/**
		 * Method which contains realization inner join.
		 */
		public abstract V doInnerJoin(K key, L left, R right);

		/**
		 * Method which contains realization left join
		 */
		public @Nullable V doLeftJoin(K key, L left) {
			return null;
		}

		@Override
		public final void onInnerJoin(K key, L left, R right, StreamDataAcceptor<V> output) {
			V result = doInnerJoin(key, left, right);
			if (result != null) {
				output.accept(result);
			}
		}

		@Override
		public final void onLeftJoin(K key, L left, StreamDataAcceptor<V> output) {
			V result = doLeftJoin(key, left);
			if (result != null) {
				output.accept(result);
			}
		}
	}

	private final Comparator<K> keyComparator;
	private final Input<L> left;
	private final Input<R> right;
	private final Output output;

	private final ArrayDeque<L> leftDeque = new ArrayDeque<>();
	private final ArrayDeque<R> rightDeque = new ArrayDeque<>();

	private final Function<L, K> leftKeyFunction;
	private final Function<R, K> rightKeyFunction;

	private final Joiner<K, L, R, V> joiner;

	private StreamJoin(@NotNull Comparator<K> keyComparator,
			@NotNull Function<L, K> leftKeyFunction, @NotNull Function<R, K> rightKeyFunction,
			@NotNull Joiner<K, L, R, V> joiner) {
		this.keyComparator = keyComparator;
		this.joiner = joiner;
		this.left = new Input<>(leftDeque);
		this.right = new Input<>(rightDeque);
		this.leftKeyFunction = leftKeyFunction;
		this.rightKeyFunction = rightKeyFunction;
		this.output = new Output();
	}

	/**
	 * Creates a new instance of StreamJoin
	 *
	 * @param keyComparator    comparator for compare keys
	 * @param leftKeyFunction  function for counting keys of left stream
	 * @param rightKeyFunction function for counting keys of right stream
	 * @param joiner           joiner which will join streams
	 */
	public static <K, L, R, V> StreamJoin<K, L, R, V> create(Comparator<K> keyComparator,
			Function<L, K> leftKeyFunction, Function<R, K> rightKeyFunction,
			Joiner<K, L, R, V> joiner) {
		return new StreamJoin<>(keyComparator, leftKeyFunction, rightKeyFunction, joiner);
	}

	private final class Input<I> extends AbstractStreamConsumer<I> implements StreamDataAcceptor<I> {
		private final Deque<I> deque;

		public Input(Deque<I> deque) {
			this.deque = deque;
		}

		@Override
		public void accept(I item) {
			boolean wasEmpty = deque.isEmpty();
			deque.addLast(item);
			if (wasEmpty) {
				output.join();
			}
		}

		@Override
		protected void onStarted() {
			output.join();
		}

		@Override
		protected void onEndOfStream() {
			output.join();
			output.getAcknowledgement()
					.whenResult(this::acknowledge)
					.whenException(this::closeEx);
		}

		@Override
		protected void onError(Exception e) {
			output.closeEx(e);
		}
	}

	private final class Output extends AbstractStreamSupplier<V> {

		void join() {
			resume();
		}

		@Override
		protected void onResumed() {
			StreamDataAcceptor<V> acceptor = this::send;
			if (isReady() && !leftDeque.isEmpty() && !rightDeque.isEmpty()) {
				L leftValue = leftDeque.peek();
				K leftKey = leftKeyFunction.apply(leftValue);
				R rightValue = rightDeque.peek();
				K rightKey = rightKeyFunction.apply(rightValue);
				while (true) {
					int compare = keyComparator.compare(leftKey, rightKey);
					if (compare < 0) {
						joiner.onLeftJoin(leftKey, leftValue, acceptor);
						leftDeque.poll();
						if (leftDeque.isEmpty())
							break;
						leftValue = leftDeque.peek();
						leftKey = leftKeyFunction.apply(leftValue);
					} else if (compare > 0) {
						rightDeque.poll();
						if (rightDeque.isEmpty())
							break;
						rightValue = rightDeque.peek();
						rightKey = rightKeyFunction.apply(rightValue);
					} else {
						joiner.onInnerJoin(leftKey, leftValue, rightValue, acceptor);
						leftDeque.poll();
						if (leftDeque.isEmpty())
							break;
						if (!isReady())
							break;
						leftValue = leftDeque.peek();
						leftKey = leftKeyFunction.apply(leftValue);
					}
				}
			}
			if (isReady()) {
				if (left.isEndOfStream() && right.isEndOfStream()) {
					sendEndOfStream();
				} else {
					left.resume(left);
					right.resume(right);
				}
			} else {
				left.suspend();
				right.suspend();
			}
		}

		@Override
		protected void onError(Exception e) {
			left.closeEx(e);
			right.closeEx(e);
		}

		@Override
		protected void onCleanup() {
			leftDeque.clear();
			rightDeque.clear();
		}
	}

	/**
	 * Returns left stream
	 */
	public StreamConsumer<L> getLeft() {
		return left;
	}

	/**
	 * Returns right stream
	 */
	public StreamConsumer<R> getRight() {
		return right;
	}

	@Override
	public List<? extends StreamConsumer<?>> getInputs() {
		return asList(left, right);
	}

	@Override
	public StreamSupplier<V> getOutput() {
		return output;
	}
}
