package io.activej.datastream.processor.reducer;

import io.activej.datastream.processor.reducer.impl.AccumulatorToAccumulator;
import io.activej.datastream.processor.reducer.impl.AccumulatorToOutput;
import io.activej.datastream.processor.reducer.impl.InputToAccumulator;
import io.activej.datastream.processor.reducer.impl.InputToOutput;

/**
 * Represents a helpful class which contains methods for simple casting types of input and
 * output streams
 *
 * @param <K> type of keys
 * @param <I> type of input data
 * @param <O> type of output data
 * @param <A> type of accumulator
 */
public abstract class ReducerToResult<K, I, O, A> {

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

}
