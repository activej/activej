package io.activej.dataflow.calcite.aggregation;

import org.jetbrains.annotations.Nullable;

public abstract class AbstractSumReducer<I, T> extends FieldReducer<I, T, T> {
	public AbstractSumReducer(int fieldIndex, @Nullable String fieldAlias) {
		super(fieldIndex, fieldAlias);
	}

	@Override
	public final String doGetName(String fieldName) {
		return "SUM(" + fieldName + ')';
	}

	@Override
	public final Class<T> getResultClass(Class<T> accumulatorClass) {
		return accumulatorClass;
	}

	@Override
	public final T produceResult(T accumulator) {
		return accumulator;
	}
}
