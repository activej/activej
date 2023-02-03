package io.activej.datastream.processor.reducer;

/**
 * Represents a reducer which streams accumulator
 *
 * @param <K> type of keys
 * @param <I> type of input data
 * @param <A> type of accumulator and type of output data
 */
public abstract class ReducerToAccumulator<K, I, A> extends ReducerToResult<K, I, A, A> {
	@Override
	public final A produceResult(A accumulator) {
		return accumulator;
	}
}
