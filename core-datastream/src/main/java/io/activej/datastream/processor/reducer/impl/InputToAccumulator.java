package io.activej.datastream.processor.reducer.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.processor.reducer.Reducer;
import io.activej.datastream.processor.reducer.ReducerToResult;

/**
 * Represents a reducer which contains ReducerToResult where identified methods for processing
 * items . Streams obtained accumulator on complete.
 *
 * @param <K> type of keys
 * @param <I> type of input data
 * @param <O> type of output data
 * @param <A> type of accumulator
 */
@ExposedInternals
public final class InputToAccumulator<K, I, O, A> implements Reducer<K, I, A, A> {
	public final ReducerToResult<K, I, O, A> reducerToResult;

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
