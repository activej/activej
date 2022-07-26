package io.activej.dataflow.calcite.where;

import io.activej.record.Record;

import java.util.List;

public final class GtPredicate implements WherePredicate {
	private final Operand left;
	private final Operand right;

	public GtPredicate(Operand left, Operand right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public boolean test(Record record) {
		Comparable<Object> leftValue = left.getValue(record);
		if (leftValue == null) return false;

		Comparable<Object> rightValue = right.getValue(record);
		if (rightValue == null) return false;

		return leftValue.compareTo(rightValue) > 0;
	}

	@Override
	public WherePredicate materialize(List<Object> params) {
		return new GtPredicate(
				left.materialize(params),
				right.materialize(params)
		);
	}

	public Operand getLeft() {
		return left;
	}

	public Operand getRight() {
		return right;
	}

	@Override
	public String toString() {
		return "GtPredicate[" +
				"left=" + left + ", " +
				"right=" + right + ']';
	}

}
