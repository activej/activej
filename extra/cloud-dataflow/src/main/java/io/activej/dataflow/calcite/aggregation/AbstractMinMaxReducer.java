package io.activej.dataflow.calcite.aggregation;

import io.activej.codegen.util.Primitives;
import io.activej.record.Record;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractMinMaxReducer<I extends Comparable<I>> extends FieldReducer<I, I, I> {

	protected AbstractMinMaxReducer(int fieldIndex, @Nullable String fieldAlias) {
		super(fieldIndex, fieldAlias);
	}

	protected abstract I compare(@NotNull I current, @NotNull I candidate);

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
	protected final I doAccumulate(I accumulator, @NotNull I fieldValue) {
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
