package io.activej.dataflow.calcite.utils;

import io.activej.datastream.processor.reducer.Reducer;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.record.Record;

public final class NamedReducer implements Reducer<Record, Record, Record, Object> {
	private final String tableName;
	private final Reducer<Record, Record, Record, Object> reducer;

	public NamedReducer(String tableName, Reducer<Record, Record, Record, Object> reducer) {
		this.tableName = tableName;
		this.reducer = reducer;
	}

	public String getTableName() {
		return tableName;
	}

	@Override
	public Object onFirstItem(StreamDataAcceptor<Record> stream, Record key, Record firstValue) {
		return reducer.onFirstItem(stream, key, firstValue);
	}

	@Override
	public Object onNextItem(StreamDataAcceptor<Record> stream, Record key, Record nextValue, Object accumulator) {
		return reducer.onNextItem(stream, key, nextValue, accumulator);
	}

	@Override
	public void onComplete(StreamDataAcceptor<Record> stream, Record key, Object accumulator) {
		reducer.onComplete(stream, key, accumulator);
	}
}
