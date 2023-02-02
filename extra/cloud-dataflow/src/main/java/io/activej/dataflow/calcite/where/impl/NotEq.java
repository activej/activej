package io.activej.dataflow.calcite.where.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.where.WherePredicate;
import io.activej.record.Record;

import java.util.List;

import static io.activej.dataflow.calcite.utils.Utils.equalsUnknown;

@ExposedInternals
public final class NotEq implements WherePredicate {
	public final Operand<?> left;
	public final Operand<?> right;

	public NotEq(Operand<?> left, Operand<?> right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public boolean test(Record record) {
		Object leftValue = left.getValue(record);
		if (leftValue == null) return false;

		Object rightValue = right.getValue(record);
		if (rightValue == null) return false;

		return !equalsUnknown(leftValue, rightValue);
	}

	@Override
	public WherePredicate materialize(List<Object> params) {
		return new NotEq(
				left.materialize(params),
				right.materialize(params)
		);
	}

	@Override
	public String toString() {
		return "NotEq[" +
				"left=" + left + ", " +
				"right=" + right + ']';
	}

}
