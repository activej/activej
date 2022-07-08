package io.activej.dataflow.calcite.where;

import io.activej.record.Record;

public final class LePredicate implements WherePredicate {
	private final Operand left;
	private final Operand right;

	public LePredicate(Operand left, Operand right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public boolean test(Record record) {
		Comparable<Object> leftValue = left.getValue(record);
		if (leftValue == null) return false;

		Comparable<Object> rightValue = right.getValue(record);
		if (rightValue == null) return false;

		return leftValue.compareTo(rightValue) <= 0;
	}

	public Operand getLeft() {
		return left;
	}

	public Operand getRight() {
		return right;
	}

	@Override
	public String toString() {
		return "LePredicate[" +
				"left=" + left + ", " +
				"right=" + right + ']';
	}

}
