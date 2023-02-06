package io.activej.datastream.processor.reducer.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.datastream.processor.reducer.Reducer;
import io.activej.datastream.processor.reducer.ReducerToResult;
import io.activej.datastream.supplier.StreamDataAcceptor;

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
@ExposedInternals
public final class InputToOutput<K, I, O, A> implements Reducer<K, I, O, A> {
	public final ReducerToResult<K, I, O, A> reducerToResult;

	/**
	 * Creates a new instance of InputToOutput with  ReducerToResult from arguments
	 */
	public InputToOutput(ReducerToResult<K, I, O, A> reducerToResult) {
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
