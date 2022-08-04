package io.activej.dataflow.calcite.aggregation;

import io.activej.record.RecordScheme;
import org.jetbrains.annotations.NotNull;

public final class SumReducerInteger<I extends Number> extends AbstractSumReducer<I, Long> {
	public SumReducerInteger(int fieldIndex) {
		super(fieldIndex);
	}

	@Override
	public Class<Long> getAccumulatorClass(Class<I> inputClass) {
		return long.class;
	}

	@Override
	public Long createAccumulator(RecordScheme key) {
		return 0L;
	}

	@Override
	protected Long doAccumulate(Long accumulator, @NotNull I fieldValue) {
		return accumulator + fieldValue.longValue();
	}

	@Override
	public Long combine(Long accumulator, Long anotherAccumulator) {
		return accumulator + anotherAccumulator;
	}
}
