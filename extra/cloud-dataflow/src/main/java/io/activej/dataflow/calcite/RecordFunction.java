package io.activej.dataflow.calcite;

import io.activej.record.Record;
import io.activej.record.RecordScheme;

import java.util.function.Function;

public interface RecordFunction<T> extends Function<T, Record> {
	RecordScheme getScheme();
}
