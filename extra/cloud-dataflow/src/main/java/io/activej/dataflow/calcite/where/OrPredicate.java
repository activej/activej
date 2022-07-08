package io.activej.dataflow.calcite.where;

import io.activej.record.Record;

import java.util.List;

public record OrPredicate(List<WherePredicate> predicates) implements WherePredicate {

	@Override
	public boolean test(Record record) {
		for (WherePredicate predicate : predicates) {
			if (predicate.test(record)) return true;
		}
		return false;
	}

}
