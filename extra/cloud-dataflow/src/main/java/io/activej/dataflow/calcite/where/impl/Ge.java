package io.activej.dataflow.calcite.where.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.where.WherePredicate;
import io.activej.record.Record;

import java.util.List;

import static io.activej.dataflow.calcite.utils.Utils.compareToUnknown;

@ExposedInternals
public final class Ge implements WherePredicate {
	public final Operand<?> left;
	public final Operand<?> right;

	public Ge(Operand<?> left, Operand<?> right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public boolean test(Record record) {
		Comparable<Object> leftValue = left.getValue(record);
		if (leftValue == null) return false;

		Comparable<Object> rightValue = right.getValue(record);
		if (rightValue == null) return false;

		return compareToUnknown(leftValue, rightValue) >= 0;
	}

	@Override
	public WherePredicate materialize(List<Object> params) {
		return new Ge(
				left.materialize(params),
				right.materialize(params)
		);
	}

	@Override
	public String toString() {
		return "Ge[" +
				"left=" + left + ", " +
				"right=" + right + ']';
	}

}
