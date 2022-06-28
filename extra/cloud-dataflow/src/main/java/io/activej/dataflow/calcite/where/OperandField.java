package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

public final class OperandField<T> implements Operand<T> {
	private final int index;

	public OperandField(@Deserialize("index") int index) {
		this.index = index;
	}

	@Override
	public T getValue(Record record) {
		return record.get(index);
	}

	@Serialize(order = 1)
	public int getIndex() {
		return index;
	}
}
