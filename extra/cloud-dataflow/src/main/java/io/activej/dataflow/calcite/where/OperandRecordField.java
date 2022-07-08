package io.activej.dataflow.calcite.where;

import io.activej.record.Record;

public final class OperandRecordField implements Operand {
	private final int index;

	public OperandRecordField(int index) {
		this.index = index;
	}

	@Override
	public <T> T getValue(Record record) {
		return record.get(index);
	}

	public int getIndex() {return index;}

	@Override
	public String toString() {
		return "OperandRecordField[" +
				"index=" + index + ']';
	}

}
