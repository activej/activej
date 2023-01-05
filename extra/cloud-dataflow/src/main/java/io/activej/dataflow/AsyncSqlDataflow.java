package io.activej.dataflow;

import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.record.Record;

public interface AsyncSqlDataflow {
	Promise<StreamSupplier<Record>> query(String sql);
}
