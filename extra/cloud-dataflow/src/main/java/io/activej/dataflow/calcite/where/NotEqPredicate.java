package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

public final class NotEqPredicate implements WherePredicate {
	private final Operand left;
	private final Operand right;

	public NotEqPredicate(@Deserialize("left") Operand left, @Deserialize("right") Operand right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public boolean test(Record record) {
		Object leftValue = left.getValue(record);
		if (leftValue == null) return false;

		Object rightValue = right.getValue(record);
		if (rightValue == null) return false;

		return !leftValue.equals(rightValue);
	}

	@Serialize(order = 1)
	public Operand getLeft() {
		return left;
	}

	@Serialize(order = 2)
	public Operand getRight() {
		return right;
	}
}
