package io.activej.dataflow.calcite.aggregation;

import io.activej.datastream.processor.StreamReducers;
import io.activej.record.Record;
import io.activej.record.RecordScheme;

import java.util.List;

public final class RecordReducer extends StreamReducers.ReducerToResult<RecordScheme, Record, Record, Record> {
	private final List<FieldReducer<Object, Object, Object>> reducers;

	public RecordReducer(List<FieldReducer<?, ?, ?>> reducers) {
		//noinspection unchecked
		this.reducers = (List<FieldReducer<Object, Object, Object>>) (List<?>) reducers;
	}

	@Override
	public Record createAccumulator(RecordScheme originalScheme) {
		RecordScheme accumulatorScheme = createAccumulatorScheme(originalScheme);

		Record accumulator = accumulatorScheme.record();

		for (int i = 0; i < reducers.size(); i++) {
			FieldReducer<Object, Object, Object> reducer = reducers.get(i);
			Object fieldAccumulator = reducer.createAccumulator(originalScheme);
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
		RecordScheme outputScheme = createOutputScheme(accumulator.getScheme());
		Record result = outputScheme.record();

		for (int i = 0, reducersSize = reducers.size(); i < reducersSize; i++) {
			FieldReducer<Object, Object, Object> reducer = reducers.get(i);
			Object fieldResult = reducer.produceResult(accumulator.get(i));
			result.set(i, fieldResult);
		}

		return result;
	}

	public List<FieldReducer<Object, Object, Object>> getReducers() {
		return reducers;
	}

	private RecordScheme createAccumulatorScheme(RecordScheme originalScheme) {
		RecordScheme resultScheme = RecordScheme.create(originalScheme.getClassLoader());

		for (FieldReducer<Object, Object, Object> reducer : reducers) {
			int fieldIndex = reducer.getFieldIndex();
			String fieldName = fieldIndex == -1 ? "*" : originalScheme.getField(fieldIndex);
			String resultFieldName = reducer.getName() + '(' + fieldName + ')';

			//noinspection unchecked
			Class<Object> fieldType = (Class<Object>) (fieldIndex == -1 ? long.class : originalScheme.getFieldType(fieldIndex));
			Class<Object> resultClass = reducer.getAccumulatorClass(fieldType);
			resultScheme.addField(resultFieldName, resultClass);
		}

		return resultScheme
				.withComparator(resultScheme.getFields())
				.build();
	}

	private RecordScheme createOutputScheme(RecordScheme accumulatorScheme) {
		RecordScheme outputScheme = RecordScheme.create(accumulatorScheme.getClassLoader());

		for (int i = 0; i < accumulatorScheme.size(); i++) {
			String accumulatorFieldName = accumulatorScheme.getField(i);

			//noinspection unchecked
			Class<Object> accumulatorFieldType = (Class<Object>) accumulatorScheme.getFieldType(i);
			FieldReducer<Object, Object, Object> reducer = reducers.get(i);
			Class<?> outputFieldClass = reducer.getResultClass(accumulatorFieldType);

			outputScheme.addField(accumulatorFieldName, outputFieldClass);
		}

		return outputScheme
				.withComparator(outputScheme.getFields())
				.build();
	}

	public RecordScheme createScheme(RecordScheme originalScheme) {
		return createOutputScheme(createAccumulatorScheme(originalScheme));
	}

	public InputToAccumulator<RecordScheme, Record, Record, Record> getInputToAccumulator() {
		return new InputToAccumulator<>(this);
	}

	public AccumulatorToOutput<RecordScheme, Record, Record, Record> getAccumulatorToOutput() {
		return new AccumulatorToOutput<>(this);
	}
}
