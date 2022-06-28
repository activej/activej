package io.activej.dataflow.calcite;

import io.activej.record.Record;

import java.util.function.Function;

public interface RecordFunction<T> extends Function<T, Record> {
}
