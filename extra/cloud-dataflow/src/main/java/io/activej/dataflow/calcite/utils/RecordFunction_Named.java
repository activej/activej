package io.activej.dataflow.calcite.utils;

import io.activej.dataflow.calcite.RecordFunction;
import io.activej.record.Record;
import io.activej.record.RecordScheme;

public class RecordFunction_Named<T> implements RecordFunction<T> {
	private final String tableName;
	private final RecordFunction<T> recordFunction;

	public RecordFunction_Named(String tableName, RecordFunction<T> recordFunction) {
		this.tableName = tableName;
		this.recordFunction = recordFunction;
	}

	public String getTableName() {
		return tableName;
	}

	@Override
	public RecordScheme getScheme() {
		return recordFunction.getScheme();
	}

	@Override
	public Record apply(T t) {
		return recordFunction.apply(t);
	}
}
