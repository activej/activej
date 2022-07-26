package io.activej.dataflow.calcite.where;

import io.activej.record.Record;

import java.util.List;
import java.util.function.Predicate;

public interface WherePredicate extends Predicate<Record> {
	WherePredicate materialize(List<Object> params);
}
