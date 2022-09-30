package io.activej.dataflow.calcite.utils;

import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.record.Record;

public class NamedReducer implements Reducer<Record, Record, Record, Record> {
	private final String tableName;
	private final Reducer<Record, Record, Record, Record> reducer;

	public NamedReducer(String tableName, Reducer<Record, Record, Record, Record> reducer) {
		this.tableName = tableName;
		this.reducer = reducer;
	}

	public String getTableName() {
		return tableName;
	}

	@Override
	public Record onFirstItem(StreamDataAcceptor<Record> stream, Record key, Record firstValue) {
		return reducer.onFirstItem(stream, key, firstValue);
	}

	@Override
	public Record onNextItem(StreamDataAcceptor<Record> stream, Record key, Record nextValue, Record accumulator) {
		return reducer.onNextItem(stream, key, nextValue, accumulator);
	}

	@Override
	public void onComplete(StreamDataAcceptor<Record> stream, Record key, Record accumulator) {
		reducer.onComplete(stream, key, accumulator);
	}
}
