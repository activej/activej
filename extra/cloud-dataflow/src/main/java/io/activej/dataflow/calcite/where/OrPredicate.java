package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

import java.util.List;

public final class OrPredicate implements WherePredicate {
	private final List<WherePredicate> predicates;

	public OrPredicate(@Deserialize("predicates") List<WherePredicate> predicates) {
		this.predicates = predicates;
	}

	@Override
	public boolean test(Record record) {
		for (WherePredicate predicate : predicates) {
			if (predicate.test(record)) return true;
		}
		return false;
	}

	@Serialize(order = 1)
	public List<WherePredicate> getPredicates() {
		return predicates;
	}
}
