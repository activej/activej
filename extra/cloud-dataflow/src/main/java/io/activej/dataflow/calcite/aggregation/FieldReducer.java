package io.activej.dataflow.calcite.aggregation;

import io.activej.datastream.processor.StreamReducers;
import io.activej.record.Record;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class FieldReducer<I, O, A> extends StreamReducers.ReducerToResult<Record, Record, O, A> {
	private final int fieldIndex;
	private final @Nullable String fieldAlias;

	protected FieldReducer(int fieldIndex, @Nullable String fieldAlias) {
		this.fieldIndex = fieldIndex;
		this.fieldAlias = fieldAlias;
	}

	public abstract Class<O> getResultClass(Class<A> accumulatorClass);

	public abstract Class<A> getAccumulatorClass(Class<I> inputClass);

	public abstract String doGetName(String fieldName);

	protected abstract A doAccumulate(A accumulator, @NotNull I fieldValue);

	public final String getName(String fieldName) {
		if (fieldAlias != null) return fieldAlias;

		return doGetName(fieldName);
	}

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

	public @Nullable String getFieldAlias() {
		return fieldAlias;
	}
}
