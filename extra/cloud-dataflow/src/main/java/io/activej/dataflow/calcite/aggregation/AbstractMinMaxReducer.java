package io.activej.dataflow.calcite.aggregation;

import io.activej.record.Record;
import io.activej.types.Primitives;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractMinMaxReducer<I extends Comparable<I>> extends FieldReducer<I, I, I> {

	protected AbstractMinMaxReducer(int fieldIndex, @Nullable String fieldAlias) {
		super(fieldIndex, fieldAlias);
	}

	protected abstract I compare(I current, I candidate);

	@Override
	public final Class<I> getResultClass(Class<I> accumulatorClass) {
		return accumulatorClass;
	}

	@Override
	public final Class<I> getAccumulatorClass(Class<I> inputClass) {
		return Primitives.wrap(inputClass);
	}

	@Override
	public final I createAccumulator(Record key) {
		return null;
	}

	@Override
	protected final I doAccumulate(I accumulator, I fieldValue) {
		if (accumulator == null) return fieldValue;

		return compare(accumulator, fieldValue);
	}

	@Override
	public final I combine(I accumulator, I anotherAccumulator) {
		return compare(accumulator, anotherAccumulator);
	}

	@Override
	public final I produceResult(I accumulator) {
		return accumulator;
	}
}
