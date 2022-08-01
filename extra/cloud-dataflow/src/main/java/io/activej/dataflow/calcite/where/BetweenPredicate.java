package io.activej.dataflow.calcite.where;

import io.activej.dataflow.calcite.operand.Operand;
import io.activej.record.Record;

import java.util.List;

public final class BetweenPredicate implements WherePredicate {
	private final Operand<?> value;
	private final Operand<?> from;
	private final Operand<?> to;

	public BetweenPredicate(Operand<?> value, Operand<?> from, Operand<?> to) {
		this.value = value;
		this.from = from;
		this.to = to;
	}

	@Override
	public boolean test(Record record) {
		Comparable<Object> value = this.value.getValue(record);
		if (value == null) return false;

		Comparable<Object> from = this.from.getValue(record);
		if (from == null) return false;

		Comparable<Object> to = this.to.getValue(record);
		if (to == null) return false;

		return value.compareTo(from) >= 0 && value.compareTo(to) <= 0;
	}

	@Override
	public WherePredicate materialize(List<Object> params) {
		return new BetweenPredicate(
				value.materialize(params),
				from.materialize(params),
				to.materialize(params)
		);
	}

	public Operand<?> getValue() {
		return value;
	}

	public Operand<?> getFrom() {
		return from;
	}

	public Operand<?> getTo() {
		return to;
	}

	@Override
	public String toString() {
		return "BetweenPredicate[" +
				"value=" + value + ", " +
				"from=" + from + ", " +
				"to=" + to + ']';
	}

}
