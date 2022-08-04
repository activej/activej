package io.activej.dataflow.calcite.aggregation;

import io.activej.record.RecordScheme;
import org.jetbrains.annotations.NotNull;

public class CountReducer<I> extends FieldReducer<I, Long, Long> {
	public CountReducer(int fieldIndex) {
		super(fieldIndex);
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
	public String getName() {
		return "COUNT";
	}

	@Override
	public Long createAccumulator(RecordScheme key) {
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
