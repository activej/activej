package io.activej.dataflow.calcite.where;

import io.activej.record.Record;

import java.util.List;

public final class OperandListGet implements Operand {
	private final Operand listOperand;
	private final Operand indexOperand;

	public OperandListGet(Operand listOperand, Operand indexOperand) {
		this.listOperand = listOperand;
		this.indexOperand = indexOperand;
	}

	@Override
	public <T> T getValue(Record record) {
		List<T> list = listOperand.getValue(record);
		Integer index = indexOperand.getValue(record);

		if (list == null || index == null || list.size() <= index) return null;

		return list.get(index);
	}

	public Operand getListOperand() {
		return listOperand;
	}

	public Operand getIndexOperand() {
		return indexOperand;
	}

	@Override
	public String toString() {
		return "OperandListGet[" +
				"listOperand=" + listOperand + ", " +
				"indexOperand=" + indexOperand + ']';
	}

}
