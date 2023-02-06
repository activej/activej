package io.activej.datastream.processor.reducer;

import io.activej.datastream.supplier.StreamDataAcceptor;

public abstract class BinaryAccumulatorReducer<K, T> implements Reducer<K, T, T, T> {
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
