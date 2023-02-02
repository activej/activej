package io.activej.dataflow.calcite.where.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.where.WherePredicate;
import io.activej.record.Record;

import java.util.List;

@ExposedInternals
public final class IsNotNull implements WherePredicate {
	public final Operand<?> value;

	public IsNotNull(Operand<?> value) {
		this.value = value;
	}

	@Override
	public boolean test(Record record) {
		return value.getValue(record) != null;
	}

	@Override
	public WherePredicate materialize(List<Object> params) {
		return new IsNotNull(value.materialize(params));
	}

	@Override
	public String toString() {
		return "IsNotNull[value=" + value + ']';
	}
}
