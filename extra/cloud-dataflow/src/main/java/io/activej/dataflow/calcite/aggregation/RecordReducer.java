package io.activej.dataflow.calcite.aggregation;

import io.activej.datastream.processor.reducer.ReducerToResult;
import io.activej.datastream.processor.reducer.impl.AccumulatorToOutput;
import io.activej.datastream.processor.reducer.impl.InputToAccumulator;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.record.RecordScheme.Builder;

import java.util.ArrayList;
import java.util.List;

public final class RecordReducer extends ReducerToResult<Record, Record, Record, Record> {
	private final RecordScheme originalScheme;
	private final List<FieldReducer<Object, Object, Object>> reducers;

	private final RecordScheme accumulatorScheme;
	private final RecordScheme outputScheme;

	private RecordReducer(RecordScheme originalScheme, RecordScheme accumulatorScheme, RecordScheme outputScheme, List<FieldReducer<?, ?, ?>> reducers) {
		this.originalScheme = originalScheme;
		this.accumulatorScheme = accumulatorScheme;
		this.outputScheme = outputScheme;

		//noinspection unchecked
		this.reducers = (List<FieldReducer<Object, Object, Object>>) (List<?>) reducers;
	}

	public static RecordReducer create(RecordScheme originalScheme, List<FieldReducer<?, ?, ?>> reducers) {
		RecordScheme accumulatorScheme = createAccumulatorScheme(originalScheme, reducers);
		RecordScheme outputScheme = createOutputScheme(accumulatorScheme, reducers);

		return new RecordReducer(originalScheme, accumulatorScheme, outputScheme, reducers);
	}

	@Override
	public Record createAccumulator(Record key) {
		Record accumulator = accumulatorScheme.record();

		for (int i = 0; i < reducers.size(); i++) {
			FieldReducer<Object, Object, Object> reducer = reducers.get(i);
			Object fieldAccumulator = reducer.createAccumulator(key);
			accumulator.set(i, fieldAccumulator);
		}

		return accumulator;
	}

	@Override
	public Record accumulate(Record accumulator, Record value) {
		for (int i = 0, reducersSize = reducers.size(); i < reducersSize; i++) {
			FieldReducer<Object, Object, Object> reducer = reducers.get(i);
			Object fieldAccumulated = reducer.accumulate(accumulator.get(i), value);
			accumulator.set(i, fieldAccumulated);
		}

		return accumulator;
	}

	@Override
	public Record combine(Record accumulator, Record anotherAccumulator) {
		RecordScheme scheme = accumulator.getScheme();
		assert scheme == anotherAccumulator.getScheme();

		Record newAccumulator = scheme.record();

		for (int i = 0; i < reducers.size(); i++) {
			FieldReducer<Object, Object, Object> reducer = reducers.get(i);
			Object combined = reducer.combine(accumulator.get(i), anotherAccumulator.get(i));
			newAccumulator.set(i, combined);
		}

		return newAccumulator;
	}

	@Override
	public Record produceResult(Record accumulator) {
		Record result = outputScheme.record();

		for (int i = 0, reducersSize = reducers.size(); i < reducersSize; i++) {
			FieldReducer<Object, Object, Object> reducer = reducers.get(i);
			Object fieldResult = reducer.produceResult(accumulator.get(i));
			result.set(i, fieldResult);
		}

		return result;
	}

	public RecordScheme getOriginalScheme() {
		return originalScheme;
	}

	public RecordScheme getAccumulatorScheme() {
		return accumulatorScheme;
	}

	public RecordScheme getOutputScheme() {
		return outputScheme;
	}

	public List<FieldReducer<Object, Object, Object>> getReducers() {
		return reducers;
	}

	private static RecordScheme createAccumulatorScheme(RecordScheme originalScheme, List<FieldReducer<?, ?, ?>> fieldReducers) {
		Builder accumulatorSchemeBuilder = RecordScheme.builder(originalScheme.getClassLoader());

		List<String> fields = new ArrayList<>(fieldReducers.size());
		for (FieldReducer<?, ?, ?> reducer : fieldReducers) {
			int fieldIndex = reducer.getFieldIndex();
			String fieldName = fieldIndex == -1 ? "*" : originalScheme.getField(fieldIndex);
			String resultFieldName = reducer.getName(fieldName);

			Class<?> fieldType = (Class<?>) (fieldIndex == -1 ? long.class : originalScheme.getFieldType(fieldIndex));
			//noinspection unchecked,rawtypes
			Class<Object> resultClass = reducer.getAccumulatorClass((Class) fieldType);
			accumulatorSchemeBuilder.withField(resultFieldName, resultClass);
			fields.add(resultFieldName);
		}

		return accumulatorSchemeBuilder
			.withComparatorFields(fields)
			.build();
	}

	private static RecordScheme createOutputScheme(RecordScheme accumulatorScheme, List<FieldReducer<?, ?, ?>> fieldReducers) {
		Builder outputSchemeBuilder = RecordScheme.builder(accumulatorScheme.getClassLoader());

		List<String> fields = new ArrayList<>(accumulatorScheme.size());
		for (int i = 0; i < accumulatorScheme.size(); i++) {
			String accumulatorFieldName = accumulatorScheme.getField(i);

			Class<?> accumulatorFieldType = (Class<?>) accumulatorScheme.getFieldType(i);
			FieldReducer<?, ?, ?> reducer = fieldReducers.get(i);
			//noinspection unchecked,rawtypes
			Class<?> outputFieldClass = reducer.getResultClass((Class) accumulatorFieldType);

			outputSchemeBuilder.withField(accumulatorFieldName, outputFieldClass);
			fields.add(accumulatorFieldName);
		}

		return outputSchemeBuilder
			.withComparatorFields(fields)
			.build();
	}

	public InputToAccumulator<Record, Record, Record, Record> getInputToAccumulator() {
		return new InputToAccumulator<>(this);
	}

	public AccumulatorToOutput<Record, Record, Record, Record> getAccumulatorToOutput() {
		return new AccumulatorToOutput<>(this);
	}
}
