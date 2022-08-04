package io.activej.dataflow.calcite.utils;

import io.activej.record.Record;
import io.activej.record.RecordScheme;

import java.util.function.Function;

public class RecordSchemeFunction implements Function<Record, RecordScheme> {
	private static final RecordSchemeFunction INSTANCE = new RecordSchemeFunction();

	private RecordSchemeFunction() {
	}

	public static RecordSchemeFunction getInstance() {
		return INSTANCE;
	}

	@Override
	public RecordScheme apply(Record record) {
		return record.getScheme();
	}
}
