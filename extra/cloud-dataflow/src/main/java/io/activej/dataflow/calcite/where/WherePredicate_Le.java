package io.activej.dataflow.calcite.where;

import io.activej.dataflow.calcite.operand.Operand;
import io.activej.record.Record;

import java.util.List;

import static io.activej.dataflow.calcite.utils.Utils.compareToUnknown;

public final class WherePredicate_Le implements WherePredicate {
	private final Operand<?> left;
	private final Operand<?> right;

	public WherePredicate_Le(Operand<?> left, Operand<?> right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public boolean test(Record record) {
		Comparable<Object> leftValue = left.getValue(record);
		if (leftValue == null) return false;

		Comparable<Object> rightValue = right.getValue(record);
		if (rightValue == null) return false;

		return compareToUnknown(leftValue, rightValue) <= 0;
	}

	@Override
	public WherePredicate materialize(List<Object> params) {
		return new WherePredicate_Le(
				left.materialize(params),
				right.materialize(params)
		);
	}

	public Operand<?> getLeft() {
		return left;
	}

	public Operand<?> getRight() {
		return right;
	}

	@Override
	public String toString() {
		return "LePredicate[" +
				"left=" + left + ", " +
				"right=" + right + ']';
	}

}
