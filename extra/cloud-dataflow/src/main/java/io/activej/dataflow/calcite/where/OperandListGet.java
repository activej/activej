package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

import java.util.List;

public final class OperandListGet<T> implements Operand {
	private final Operand listOperand;
	private final Operand indexOperand;

	public OperandListGet(@Deserialize("listOperand") Operand listOperand, @Deserialize("indexOperand") Operand indexOperand) {
		this.listOperand = listOperand;
		this.indexOperand = indexOperand;
	}

	@Override
	@SuppressWarnings("unchecked")
	public T getValue(Record record) {
		List<T> list = listOperand.getValue(record);
		Integer index = indexOperand.getValue(record);

		if (list == null || index == null || list.size() <= index) return null;

		return list.get(index);
	}

	@Serialize(order = 1)
	public Operand getListOperand() {
		return listOperand;
	}

	@Serialize(order = 2)
	public Operand getIndexOperand() {
		return indexOperand;
	}
}
