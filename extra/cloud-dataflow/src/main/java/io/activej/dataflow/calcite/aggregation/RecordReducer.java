package io.activej.dataflow.calcite.aggregation;

import io.activej.datastream.processor.StreamReducers;
import io.activej.record.Record;
import io.activej.record.RecordScheme;

import java.util.List;

public final class RecordReducer extends StreamReducers.ReducerToResult<RecordScheme, Record, Record, Object[]> {
	private final List<FieldReducer<Object, Object, Object>> reducers;

	public RecordReducer(List<FieldReducer<?, ?, ?>> reducers) {
		//noinspection unchecked
		this.reducers = (List<FieldReducer<Object, Object, Object>>) (List<?>) reducers;
	}

	@Override
	public Object[] createAccumulator(RecordScheme originalScheme) {
		Object[] accumulator = new Object[reducers.size() + 1];

		int i = 0;
		for (int reducersSize = reducers.size(); i < reducersSize; i++) {
			FieldReducer<Object, Object, Object> reducer = reducers.get(i);
			Object fieldAccumulator = reducer.createAccumulator(originalScheme);
			accumulator[i] = fieldAccumulator;
		}

		accumulator[i] = createScheme(originalScheme);

		return accumulator;
	}

	@Override
	public Object[] accumulate(Object[] accumulator, Record value) {
		for (int i = 0, reducersSize = reducers.size(); i < reducersSize; i++) {
			FieldReducer<Object, Object, Object> reducer = reducers.get(i);
			Object fieldAccumulator = reducer.accumulate(accumulator[i], value);
			accumulator[i] = fieldAccumulator;
		}

		return accumulator;
	}

	@Override
	public Record produceResult(Object[] accumulator) {
		RecordScheme recordScheme = (RecordScheme) accumulator[accumulator.length - 1];
		Record record = recordScheme.record();

		for (int i = 0, reducersSize = reducers.size(); i < reducersSize; i++) {
			FieldReducer<Object, Object, Object> reducer = reducers.get(i);
			Object result = reducer.produceResult(accumulator[i]);
			record.set(i, result);
		}

		return record;
	}

	public List<FieldReducer<Object, Object, Object>> getReducers() {
		return reducers;
	}

	public RecordScheme createScheme(RecordScheme originalScheme) {
		RecordScheme resultScheme = RecordScheme.create();

		for (FieldReducer<Object, Object, Object> reducer : reducers) {
			int fieldIndex = reducer.getFieldIndex();
			String fieldName = fieldIndex == -1 ? "*" : originalScheme.getField(fieldIndex);
			String resultFieldName = reducer.getName() + '(' + fieldName + ')';

			//noinspection unchecked
			Class<Object> fieldType = (Class<Object>) (fieldIndex == -1 ? long.class : originalScheme.getFieldType(fieldIndex));
			Class<Object> resultClass = reducer.getResultClass(fieldType);
			resultScheme.addField(resultFieldName, resultClass);
		}
		return resultScheme.build();
	}

}
