package io.activej.dataflow.calcite.aggregation;

import io.activej.record.Record;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class SumReducerInteger<I extends Number> extends AbstractSumReducer<I, Long> {
	public SumReducerInteger(int fieldIndex, @Nullable String fieldAlias) {
		super(fieldIndex, fieldAlias);
	}

	@Override
	public Class<Long> getAccumulatorClass(Class<I> inputClass) {
		return long.class;
	}

	@Override
	public Long createAccumulator(Record key) {
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
