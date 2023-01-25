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

import io.activej.datastream.StreamDataAcceptor;

/**
 * Static utility methods pertaining to {@link Reducer}.
 * Contains primary ready for use reducers.
 */
public final class StreamReducers {

	/**
	 * Returns reducer which streams only one element from group of same keys.
	 *
	 * @param <K> type of key
	 * @param <T> type of output
	 */
	public static <K, T> Reducer<K, T, T, Void> deduplicateReducer() {
		return new Reducer_Deduplicate<>();
	}

	/**
	 * Returns reducer which streams all receives elements sorted by keys.
	 *
	 * @param <K> type of key
	 * @param <T> type of output
	 */
	public static <K, T> Reducer<K, T, T, Void> mergeReducer() {
		return new Reducer_Merge<>();
	}

	/**
	 * It is primary interface of Reducer.
	 *
	 * @param <K> type of keys
	 * @param <I> type of input data
	 * @param <O> type of output data
	 * @param <A> type of accumulator
	 */
	public interface Reducer<K, I, O, A> {
		/**
		 * Run when reducer received first element with key from argument.
		 *
		 * @param stream     stream where to send result
		 * @param key        key of element
		 * @param firstValue received value
		 * @return accumulator for further operations
		 */
		A onFirstItem(StreamDataAcceptor<O> stream, K key, I firstValue);

		/**
		 * Run when reducer received each next element with key from argument
		 *
		 * @param stream      stream where to send result
		 * @param key         key of element
		 * @param nextValue   received value
		 * @param accumulator accumulator which contains results of all previous operations
		 * @return accumulator for further operations
		 */
		A onNextItem(StreamDataAcceptor<O> stream, K key, I nextValue, A accumulator);

		/**
		 * Run after receiving last element with key from argument
		 *
		 * @param stream      stream where to send result
		 * @param key         key of element
		 * @param accumulator accumulator which contains results of all previous operations
		 */
		void onComplete(StreamDataAcceptor<O> stream, K key, A accumulator);
	}

	/**
	 * Represents a helpful class which contains methods for simple casting types of input and
	 * output streams
	 *
	 * @param <K> type of keys
	 * @param <I> type of input data
	 * @param <O> type of output data
	 * @param <A> type of accumulator
	 */
	public abstract static class ReducerToResult<K, I, O, A> {

		/**
		 * Creates a new accumulator with key from argument
		 *
		 * @param key key for new accumulator
		 * @return new accumulator
		 */
		public abstract A createAccumulator(K key);

		/**
		 * Processing value with accumulator
		 *
		 * @param accumulator accumulator with partials results
		 * @param value       received value for accumulating
		 * @return changing accumulator
		 */
		public abstract A accumulate(A accumulator, I value);

		/**
		 * Combines two accumulators from argument.
		 *
		 * @return new accumulator
		 */
		public A combine(A accumulator, A anotherAccumulator) {
			throw new UnsupportedOperationException("can not combine two accumulators");
		}

		/**
		 * Calls after completing receiving results for some key. It processes
		 * obtained accumulator and returns stream of output type from generic
		 *
		 * @param accumulator obtained accumulator after end receiving
		 * @return stream of output type from generic
		 */
		public abstract O produceResult(A accumulator);

		/**
		 * Creates a new reducer which receives items,accumulated it, produces after end of stream
		 * and streams result
		 */
		public final Reducer<K, I, O, A> inputToOutput() {
			return new InputToOutput<>(this);
		}

		/**
		 * Creates a new reducer which receives items,accumulated it and streams obtained accumulator
		 */
		public final Reducer<K, I, A, A> inputToAccumulator() {
			return new InputToAccumulator<>(this);
		}

		/**
		 * Creates a new reducer which receives accumulators,combines it, produces after end of stream
		 * and streams result
		 */
		public final Reducer<K, A, O, A> accumulatorToOutput() {
			return new AccumulatorToOutput<>(this);
		}

		/**
		 * Creates a new reducer which receives accumulators,combines it, and streams obtained accumulator
		 */
		public final Reducer<K, A, A, A> accumulatorToAccumulator() {
			return new AccumulatorToAccumulator<>(this);
		}

		/**
		 * Represents a reducer which contains ReducerToResult where identified methods for processing
		 * items . After searching accumulator performs some action with it with method produceResult
		 * from ReducerToResult.
		 *
		 * @param <K> type of keys
		 * @param <I> type of input data
		 * @param <O> type of output data
		 * @param <A> type of accumulator
		 */
		public static final class InputToOutput<K, I, O, A> implements Reducer<K, I, O, A> {
			private final ReducerToResult<K, I, O, A> reducerToResult;

			/**
			 * Creates a new instance of InputToOutput with  ReducerToResult from arguments
			 */
			public InputToOutput(ReducerToResult<K, I, O, A> reducerToResult) {
				this.reducerToResult = reducerToResult;
			}

			public ReducerToResult<K, I, O, A> getReducerToResult() {
				return reducerToResult;
			}

			/**
			 * Creates accumulator with ReducerToResult and accumulates with it first item
			 *
			 * @param stream     stream where to send result
			 * @param key        key of element
			 * @param firstValue received value
			 * @return accumulator with result
			 */
			@Override
			public A onFirstItem(StreamDataAcceptor<O> stream, K key, I firstValue) {
				A accumulator = reducerToResult.createAccumulator(key);
				return reducerToResult.accumulate(accumulator, firstValue);
			}

			/**
			 * Accumulates each next element.
			 *
			 * @param stream      stream where to send result
			 * @param key         key of element
			 * @param nextValue   received value
			 * @param accumulator accumulator which contains results of all previous operations
			 * @return accumulator with result
			 */
			@Override
			public A onNextItem(StreamDataAcceptor<O> stream, K key, I nextValue, A accumulator) {
				return reducerToResult.accumulate(accumulator, nextValue);
			}

			/**
			 * Produces result accumulator with ReducerToResult and streams it
			 *
			 * @param stream      stream where to send result
			 * @param key         key of element
			 * @param accumulator accumulator which contains results of all previous operations
			 */
			@Override
			public void onComplete(StreamDataAcceptor<O> stream, K key, A accumulator) {
				stream.accept(reducerToResult.produceResult(accumulator));
			}
		}

		/**
		 * Represents a reducer which contains ReducerToResult where identified methods for processing
		 * items . Streams obtained accumulator on complete.
		 *
		 * @param <K> type of keys
		 * @param <I> type of input data
		 * @param <O> type of output data
		 * @param <A> type of accumulator
		 */
		public static final class InputToAccumulator<K, I, O, A> implements Reducer<K, I, A, A> {
			private final ReducerToResult<K, I, O, A> reducerToResult;

			public ReducerToResult<K, I, O, A> getReducerToResult() {
				return reducerToResult;
			}

			/**
			 * Creates a new instance of InputToAccumulator with ReducerToResult from argument
			 */
			public InputToAccumulator(ReducerToResult<K, I, O, A> reducerToResult) {
				this.reducerToResult = reducerToResult;
			}

			/**
			 * Creates accumulator with ReducerToResult and accumulates with it first item
			 *
			 * @param stream     stream where to send result
			 * @param key        key of element
			 * @param firstValue received value
			 * @return accumulator with result
			 */
			@Override
			public A onFirstItem(StreamDataAcceptor<A> stream, K key, I firstValue) {
				A accumulator = reducerToResult.createAccumulator(key);
				return reducerToResult.accumulate(accumulator, firstValue);
			}

			/**
			 * Accumulates each next element.
			 *
			 * @param stream      stream where to send result
			 * @param key         key of element
			 * @param nextValue   received value
			 * @param accumulator accumulator which contains results of all previous operations
			 * @return accumulator with result
			 */
			@Override
			public A onNextItem(StreamDataAcceptor<A> stream, K key, I nextValue, A accumulator) {
				return reducerToResult.accumulate(accumulator, nextValue);
			}

			/**
			 * Streams obtained accumulator
			 *
			 * @param stream      stream where to send result
			 * @param key         key of element
			 * @param accumulator accumulator which contains results of all previous operations
			 */
			@Override
			public void onComplete(StreamDataAcceptor<A> stream, K key, A accumulator) {
				stream.accept(accumulator);
			}
		}

		/**
		 * Represents a reducer which contains ReducerToResult where identified methods for processing
		 * items . Each received item is accumulator, and it combines with previous value.  After
		 * searching accumulator performs some action with it with method produceResult from
		 * ReducerToResult.
		 *
		 * @param <K> type of keys
		 * @param <I> type of input data
		 * @param <O> type of output data
		 * @param <A> type of accumulator
		 */
		public static final class AccumulatorToOutput<K, I, O, A> implements Reducer<K, A, O, A> {
			private ReducerToResult<K, I, O, A> reducerToResult;

			public AccumulatorToOutput() {
			}

			/**
			 * Creates a new instance of InputToAccumulator with ReducerToResult from argument
			 */
			public AccumulatorToOutput(ReducerToResult<K, I, O, A> reducerToResult) {
				this.reducerToResult = reducerToResult;
			}

			public ReducerToResult<K, I, O, A> getReducerToResult() {
				return reducerToResult;
			}

			/**
			 * Creates accumulator which is first item
			 *
			 * @param stream     stream where to send result
			 * @param key        key of element
			 * @param firstValue received value
			 * @return accumulator with result
			 */
			@Override
			public A onFirstItem(StreamDataAcceptor<O> stream, K key, A firstValue) {
				return firstValue;
			}

			/**
			 * Combines previous accumulator and each next item
			 *
			 * @param stream      stream where to send result
			 * @param key         key of element
			 * @param nextValue   received value
			 * @param accumulator accumulator which contains results of all previous combining
			 * @return accumulator with result
			 */
			@Override
			public A onNextItem(StreamDataAcceptor<O> stream, K key, A nextValue, A accumulator) {
				return reducerToResult.combine(accumulator, nextValue);
			}

			/**
			 * Produces result accumulator with ReducerToResult and streams it
			 *
			 * @param stream      stream where to send result
			 * @param key         key of element
			 * @param accumulator accumulator which contains results of all previous operations
			 */
			@Override
			public void onComplete(StreamDataAcceptor<O> stream, K key, A accumulator) {
				stream.accept(reducerToResult.produceResult(accumulator));
			}
		}

		/**
		 * Represents  a reducer which contains ReducerToResult where identified methods for processing
		 * items . Each received item is accumulator, and it combines with previous value. Streams
		 * obtained accumulator on complete.
		 *
		 * @param <K> type of keys
		 * @param <I> type of input data
		 * @param <O> type of output data
		 * @param <A> type of accumulator
		 */
		public static final class AccumulatorToAccumulator<K, I, O, A> implements Reducer<K, A, A, A> {
			private final ReducerToResult<K, I, O, A> reducerToResult;

			public AccumulatorToAccumulator(ReducerToResult<K, I, O, A> reducerToResult) {
				this.reducerToResult = reducerToResult;
			}

			public ReducerToResult<K, I, O, A> getReducerToResult() {
				return reducerToResult;
			}

			/**
			 * Creates accumulator which is first item
			 *
			 * @param stream     stream where to send result
			 * @param key        key of element
			 * @param firstValue received value
			 * @return accumulator with result
			 */
			@Override
			public A onFirstItem(StreamDataAcceptor<A> stream, K key, A firstValue) {
				return firstValue;
			}

			/**
			 * Combines previous accumulator and each next item
			 *
			 * @param stream      stream where to send result
			 * @param key         key of element
			 * @param nextValue   received value
			 * @param accumulator accumulator which contains results of all previous combining
			 * @return accumulator with result
			 */
			@Override
			public A onNextItem(StreamDataAcceptor<A> stream, K key, A nextValue, A accumulator) {
				return reducerToResult.combine(accumulator, nextValue);
			}

			/**
			 * Streams obtained accumulator
			 *
			 * @param stream      stream where to send result
			 * @param key         key of element
			 * @param accumulator accumulator which contains results of all previous operations
			 */
			@Override
			public void onComplete(StreamDataAcceptor<A> stream, K key, A accumulator) {
				stream.accept(accumulator);
			}
		}
	}

	/**
	 * Represents a reducer which streams accumulator
	 *
	 * @param <K> type of keys
	 * @param <I> type of input data
	 * @param <A> type of accumulator and type of output data
	 */
	public abstract static class ReducerToAccumulator<K, I, A> extends ReducerToResult<K, I, A, A> {
		@Override
		public final A produceResult(A accumulator) {
			return accumulator;
		}
	}

	/**
	 * Represents a reducer which deduplicates items with same keys. Streams only one item with
	 * each key
	 *
	 * @param <K> type of keys
	 * @param <T> type of input and output data
	 */
	public static final class Reducer_Deduplicate<K, T> implements Reducer<K, T, T, Void> {
		/**
		 * On first item with new key it streams it
		 *
		 * @param stream     stream where to send result
		 * @param key        key of element
		 * @param firstValue received value
		 */
		@Override
		public Void onFirstItem(StreamDataAcceptor<T> stream, K key, T firstValue) {
			stream.accept(firstValue);
			return null;
		}

		@Override
		public Void onNextItem(StreamDataAcceptor<T> stream, K key, T nextValue, Void accumulator) {
			return null;
		}

		@Override
		public void onComplete(StreamDataAcceptor<T> stream, K key, Void accumulator) {
		}
	}

	/**
	 * Represent a reducer which streams received items sorted by keys
	 *
	 * @param <K> type of keys
	 * @param <T> type of input and output data
	 */
	public static final class Reducer_Merge<K, T> implements Reducer<K, T, T, Void> {
		@Override
		public Void onFirstItem(StreamDataAcceptor<T> stream, K key, T firstValue) {
			stream.accept(firstValue);
			return null;
		}

		@Override
		public Void onNextItem(StreamDataAcceptor<T> stream, K key, T nextValue, Void accumulator) {
			stream.accept(nextValue);
			return null;
		}

		@Override
		public void onComplete(StreamDataAcceptor<T> stream, K key, Void accumulator) {
		}
	}

	public static abstract class Reducer_BinaryAccumulator<K, T> implements Reducer<K, T, T, T> {
		@Override
		public T onFirstItem(StreamDataAcceptor<T> stream, K key, T firstValue) {
			return firstValue;
		}

		@Override
		public T onNextItem(StreamDataAcceptor<T> stream, K key, T nextValue, T accumulator) {
			return combine(key, nextValue, accumulator);
		}

		protected abstract T combine(K key, T nextValue, T accumulator);

		@Override
		public void onComplete(StreamDataAcceptor<T> stream, K key, T accumulator) {
			stream.accept(accumulator);
		}
	}
}
