package io.activej.dataflow.calcite.where.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.dataflow.calcite.where.WherePredicate;
import io.activej.record.Record;

import java.util.List;

@ExposedInternals
public final class And implements WherePredicate {
	public final List<WherePredicate> predicates;

	public And(List<WherePredicate> predicates) {
		this.predicates = predicates;
	}

	@Override
	public boolean test(Record record) {
		for (WherePredicate predicate : predicates) {
			if (!predicate.test(record)) return false;
		}
		return true;
	}

	@Override
	public WherePredicate materialize(List<Object> params) {
		return new And(
				predicates.stream()
						.map(wherePredicate -> wherePredicate.materialize(params))
						.toList()
		);
	}

	@Override
	public String toString() {
		return "And[" +
				"predicates=" + predicates + ']';
	}
}
