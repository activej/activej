package io.activej.datastream.processor.reducer;

import io.activej.datastream.supplier.StreamDataAcceptor;

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
