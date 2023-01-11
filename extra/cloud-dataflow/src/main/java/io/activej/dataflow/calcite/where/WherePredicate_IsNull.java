package io.activej.dataflow.calcite.where;

import io.activej.dataflow.calcite.operand.Operand;
import io.activej.record.Record;

import java.util.List;

public final class WherePredicate_IsNull implements WherePredicate {
	private final Operand<?> value;

	public WherePredicate_IsNull(Operand<?> value) {
		this.value = value;
	}

	@Override
	public boolean test(Record record) {
		return value.getValue(record) == null;
	}

	@Override
	public WherePredicate materialize(List<Object> params) {
		return new WherePredicate_IsNull(value.materialize(params));
	}

	public Operand<?> getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "IsNullPredicate[value=" + value + ']';
	}
}
