package io.activej.dataflow.calcite.aggregation;

import io.activej.record.Record;
import io.activej.record.RecordScheme;

import java.util.function.Function;

public class RecordSchemeFunction implements Function<Record, RecordScheme> {
	@Override
	public RecordScheme apply(Record record) {
		return record.getScheme();
	}
}
