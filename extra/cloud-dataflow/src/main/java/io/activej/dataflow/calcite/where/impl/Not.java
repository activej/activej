package io.activej.dataflow.calcite.where.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.dataflow.calcite.where.WherePredicate;
import io.activej.record.Record;

import java.util.List;

@ExposedInternals
public final class Not implements WherePredicate {
	public final WherePredicate predicate;

	public Not(WherePredicate predicate) {
		this.predicate = predicate;
	}

	@Override
	public boolean test(Record record) {
		return !predicate.test(record);
	}

	@Override
	public WherePredicate materialize(List<Object> params) {
		return new Not(predicate.materialize(params));
	}

	@Override
	public String toString() {
		return "Not[predicate=" + predicate + ']';
	}
}
