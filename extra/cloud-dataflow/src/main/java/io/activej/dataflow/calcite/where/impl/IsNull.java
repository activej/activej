package io.activej.dataflow.calcite.where.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.where.WherePredicate;
import io.activej.record.Record;

import java.util.List;

@ExposedInternals
public final class IsNull implements WherePredicate {
	public final Operand<?> value;

	public IsNull(Operand<?> value) {
		this.value = value;
	}

	@Override
	public boolean test(Record record) {
		return value.getValue(record) == null;
	}

	@Override
	public WherePredicate materialize(List<Object> params) {
		return new IsNull(value.materialize(params));
	}

	@Override
	public String toString() {
		return "IsNull[value=" + value + ']';
	}
}
