package io.activej.dataflow.calcite.where;

import io.activej.record.Record;

import java.util.List;

public final class AndPredicate implements WherePredicate {
	private final List<WherePredicate> predicates;

	public AndPredicate(List<WherePredicate> predicates) {
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
		return new AndPredicate(
				predicates.stream()
						.map(wherePredicate -> wherePredicate.materialize(params))
						.toList()
		);
	}

	public List<WherePredicate> getPredicates() {
		return predicates;
	}

	@Override
	public String toString() {
		return "AndPredicate[" +
				"predicates=" + predicates + ']';
	}
}
