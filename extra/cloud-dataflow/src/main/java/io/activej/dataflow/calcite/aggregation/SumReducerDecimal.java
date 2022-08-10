package io.activej.dataflow.calcite.aggregation;

import io.activej.record.Record;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class SumReducerDecimal<I extends Number> extends AbstractSumReducer<I, Double> {
	public SumReducerDecimal(int fieldIndex, @Nullable String fieldAlias) {
		super(fieldIndex, fieldAlias);
	}

	@Override
	public Class<Double> getAccumulatorClass(Class<I> inputClass) {
		return double.class;
	}

	@Override
	public Double createAccumulator(Record key) {
		return 0d;
	}

	@Override
	protected Double doAccumulate(Double accumulator, @NotNull I fieldValue) {
		return accumulator + fieldValue.doubleValue();
	}

	@Override
	public Double combine(Double accumulator, Double anotherAccumulator) {
		return accumulator + anotherAccumulator;
	}
}
