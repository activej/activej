package io.activej.dataflow.calcite.aggregation;

import io.activej.datastream.processor.StreamReducers;
import io.activej.record.Record;
import org.jetbrains.annotations.NotNull;

public abstract class FieldReducer<I, O, A> extends StreamReducers.ReducerToResult<Record, Record, O, A> {
	private final int fieldIndex;

	protected FieldReducer(int fieldIndex) {
		this.fieldIndex = fieldIndex;
	}

	public abstract Class<O> getResultClass(Class<A> accumulatorClass);

	public abstract Class<A> getAccumulatorClass(Class<I> inputClass);

	public abstract String getName(String fieldName);

	protected abstract A doAccumulate(A accumulator, @NotNull I fieldValue);

	@Override
	public final A accumulate(A accumulator, Record value) {
		int fieldIndex = getFieldIndex();
		if (fieldIndex == -1) {
			//noinspection unchecked
			return doAccumulate(accumulator, (I) value);
		}

		I fieldValue = value.get(fieldIndex);
		if (fieldValue == null) return accumulator;

		return doAccumulate(accumulator, fieldValue);
	}

	@Override
	public abstract A combine(A accumulator, A anotherAccumulator);

	public final int getFieldIndex() {
		return fieldIndex;
	}
}
