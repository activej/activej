package io.activej.dataflow.calcite.aggregation;

import io.activej.record.Record;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public final class KeyReducer<K> extends FieldReducer<K, K, K>{
	public KeyReducer(int fieldIndex) {
		super(fieldIndex);
	}

	@Override
	public K createAccumulator(Record key) {
		return key.get(getFieldIndex());
	}

	@Override
	public K produceResult(K accumulator) {
		return accumulator;
	}

	@Override
	public Class<K> getResultClass(Class<K> accumulatorClass) {
		return accumulatorClass;
	}

	@Override
	public Class<K> getAccumulatorClass(Class<K> inputClass) {
		return inputClass;
	}

	@Override
	public String getName(String fieldName) {
		return fieldName;
	}

	@Override
	protected K doAccumulate(K accumulator, @NotNull K fieldValue) {
		assert Objects.equals(accumulator, fieldValue);
		return accumulator;
	}

	@Override
	public K combine(K accumulator, K anotherAccumulator) {
		assert Objects.equals(accumulator, anotherAccumulator);
		return accumulator;
	}
}
