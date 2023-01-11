package io.activej.dataflow.calcite.where;

import io.activej.dataflow.calcite.operand.Operand;
import io.activej.record.Record;

import java.util.List;

import static io.activej.dataflow.calcite.utils.Utils.compareToUnknown;

public final class WherePredicate_Between implements WherePredicate {
	private final Operand<?> value;
	private final Operand<?> from;
	private final Operand<?> to;

	public WherePredicate_Between(Operand<?> value, Operand<?> from, Operand<?> to) {
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

		return compareToUnknown(value, from) >= 0 && compareToUnknown(value, to) <= 0;
	}

	@Override
	public WherePredicate materialize(List<Object> params) {
		return new WherePredicate_Between(
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
