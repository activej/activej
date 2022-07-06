package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

public final class BetweenPredicate implements WherePredicate {
	private final Operand value;
	private final Operand from;
	private final Operand to;

	public BetweenPredicate(@Deserialize("value") Operand value, @Deserialize("from") Operand from, @Deserialize("to") Operand to) {
		this.value = value;
		this.from = from;
		this.to = to;
	}

	@Override
	@SuppressWarnings({"rawtypes", "unchecked"})
	public boolean test(Record record) {
		Comparable value = (Comparable) this.value.getValue(record);
		if (value == null) return false;

		Comparable from = (Comparable) this.from.getValue(record);
		if (from == null) return false;

		Comparable to = (Comparable) this.to.getValue(record);
		if (to == null) return false;

		return value.compareTo(from) >= 0 && value.compareTo(to) <= 0;
	}

	@Serialize(order = 1)
	public Operand getValue() {
		return value;
	}

	@Serialize(order = 2)
	public Operand getFrom() {
		return from;
	}

	@Serialize(order = 3)
	public Operand getTo() {
		return to;
	}
}
