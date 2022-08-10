package io.activej.dataflow.calcite.aggregation;

import io.activej.record.Record;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CountReducer<I> extends FieldReducer<I, Long, Long> {
	public CountReducer(int fieldIndex, @Nullable String fieldAlias) {
		super(fieldIndex, fieldAlias);
	}

	@Override
	public Class<Long> getResultClass(Class<Long> accumulatorClass) {
		return accumulatorClass;
	}

	@Override
	public Class<Long> getAccumulatorClass(Class<I> inputClass) {
		return long.class;
	}

	@Override
	public String doGetName(String fieldName) {
		return "COUNT(" + fieldName + ')';
	}

	@Override
	public Long createAccumulator(Record key) {
		return 0L;
	}

	@Override
	protected Long doAccumulate(Long accumulator, @NotNull Object fieldValue) {
		return accumulator + 1;
	}

	@Override
	public Long combine(Long accumulator, Long anotherAccumulator) {
		return accumulator + anotherAccumulator;
	}

	@Override
	public Long produceResult(Long accumulator) {
		return accumulator;
	}
}
