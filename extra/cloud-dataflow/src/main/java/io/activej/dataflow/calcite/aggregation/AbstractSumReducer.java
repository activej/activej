package io.activej.dataflow.calcite.aggregation;

public abstract class AbstractSumReducer<I, T> extends FieldReducer<I, T, T> {
	public AbstractSumReducer(int fieldIndex) {
		super(fieldIndex);
	}

	@Override
	public final String getName(String fieldName) {
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
