package io.activej.dataflow.calcite.where;

import io.activej.record.Record;

import java.util.List;

public final class EqPredicate implements WherePredicate {
	private final Operand left;
	private final Operand right;

	public EqPredicate(Operand left, Operand right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public boolean test(Record record) {
		Object leftValue = left.getValue(record);
		if (leftValue == null) return false;

		Object rightValue = right.getValue(record);
		if (rightValue == null) return false;

		return leftValue.equals(rightValue);
	}

	@Override
	public WherePredicate materialize(List<Object> params) {
		return new EqPredicate(
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
		return "EqPredicate[" +
				"left=" + left + ", " +
				"right=" + right + ']';
	}

}
