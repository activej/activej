package io.activej.dataflow.calcite.where;

import io.activej.record.Record;

import java.util.List;

public final class OrPredicate implements WherePredicate {
	private final List<WherePredicate> predicates;

	public OrPredicate(List<WherePredicate> predicates) {
		this.predicates = predicates;
	}

	@Override
	public boolean test(Record record) {
		for (WherePredicate predicate : predicates) {
			if (predicate.test(record)) return true;
		}
		return false;
	}

	@Override
	public WherePredicate materialize(List<Object> params) {
		return new OrPredicate(
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
		return "OrPredicate[" +
				"predicates=" + predicates + ']';
	}
}
