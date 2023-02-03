package io.activej.datastream.processor.reducer.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.processor.reducer.Reducer;
import io.activej.datastream.processor.reducer.ReducerToResult;

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
@ExposedInternals
public final class AccumulatorToAccumulator<K, I, O, A> implements Reducer<K, A, A, A> {
	public final ReducerToResult<K, I, O, A> reducerToResult;

	public AccumulatorToAccumulator(ReducerToResult<K, I, O, A> reducerToResult) {
		this.reducerToResult = reducerToResult;
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
