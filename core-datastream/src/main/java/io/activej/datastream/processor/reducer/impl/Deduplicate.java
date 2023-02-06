package io.activej.datastream.processor.reducer.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.datastream.processor.reducer.Reducer;
import io.activej.datastream.supplier.StreamDataAcceptor;

/**
 * Represents a reducer which deduplicates items with same keys. Streams only one item with
 * each key
 *
 * @param <K> type of keys
 * @param <T> type of input and output data
 */
@ExposedInternals
public final class Deduplicate<K, T> implements Reducer<K, T, T, Void> {
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
