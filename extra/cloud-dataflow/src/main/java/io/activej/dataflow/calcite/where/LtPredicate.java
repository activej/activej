package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

public final class LtPredicate implements WherePredicate {
	private final Operand left;
	private final Operand right;

	public LtPredicate(@Deserialize("left") Operand left, @Deserialize("right") Operand right) {
		this.left = left;
		this.right = right;
	}

	@Override
	@SuppressWarnings({"rawtypes", "unchecked"})
	public boolean test(Record record) {
		Comparable leftValue = (Comparable) left.getValue(record);
		if (leftValue == null) return false;

		Comparable rightValue = (Comparable) right.getValue(record);
		if (rightValue == null) return false;

		return leftValue.compareTo(rightValue) < 0;
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
