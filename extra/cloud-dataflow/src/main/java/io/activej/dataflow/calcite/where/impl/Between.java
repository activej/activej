package io.activej.dataflow.calcite.where.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.where.WherePredicate;
import io.activej.record.Record;

import java.util.List;

import static io.activej.dataflow.calcite.utils.Utils.compareToUnknown;

@ExposedInternals
public final class Between implements WherePredicate {
	public final Operand<?> value;
	public final Operand<?> from;
	public final Operand<?> to;

	public Between(Operand<?> value, Operand<?> from, Operand<?> to) {
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
		return new Between(
				value.materialize(params),
				from.materialize(params),
				to.materialize(params)
		);
	}

	@Override
	public String toString() {
		return "Between[" +
				"value=" + value + ", " +
				"from=" + from + ", " +
				"to=" + to + ']';
	}

}
