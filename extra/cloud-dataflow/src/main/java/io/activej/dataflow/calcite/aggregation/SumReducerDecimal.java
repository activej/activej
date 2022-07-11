package io.activej.dataflow.calcite.aggregation;

import io.activej.record.RecordScheme;
import org.jetbrains.annotations.NotNull;

public final class SumReducerDecimal<I extends Number> extends AbstractSumReducer<I, Double> {
	public SumReducerDecimal(int fieldIndex) {
		super(fieldIndex);
	}

	@Override
	public Class<Double> getResultClass(Class<I> inputClass) {
		return double.class;
	}

	@Override
	public Double createAccumulator(RecordScheme key) {
		return 0d;
	}

	@Override
	protected Double doAccumulate(Double accumulator, @NotNull I fieldValue) {
		return accumulator + fieldValue.doubleValue();
	}
}
