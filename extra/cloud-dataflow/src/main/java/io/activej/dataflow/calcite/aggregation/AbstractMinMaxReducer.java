package io.activej.dataflow.calcite.aggregation;

import io.activej.record.RecordScheme;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractMinMaxReducer<I extends Comparable<I>> extends FieldReducer<I, I, I> {

	protected AbstractMinMaxReducer(int fieldIndex) {
		super(fieldIndex);
	}

	protected abstract I compare(@NotNull I current, @NotNull I candidate);

	@Override
	public final Class<I> getResultClass(Class<I> inputClass) {
		return inputClass;
	}

	@Override
	public final I createAccumulator(RecordScheme key) {
		return null;
	}

	@Override
	protected final I doAccumulate(I accumulator, @NotNull I fieldValue) {
		if (accumulator == null) return fieldValue;

		return compare(accumulator, fieldValue);
	}

	@Override
	public final I produceResult(I accumulator) {
		return accumulator;
	}
}
