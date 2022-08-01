package io.activej.dataflow.calcite.operand;

import io.activej.record.Record;
import io.activej.record.RecordScheme;
import org.apache.calcite.rex.RexDynamicParam;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;

public final class OperandRecordField implements Operand<OperandRecordField> {
	private final int index;

	public OperandRecordField(int index) {
		this.index = index;
	}

	@Override
	public <T> T getValue(Record record) {
		return record.get(index);
	}

	@Override
	public Type getFieldType(RecordScheme original) {
		return original.getFieldType(index);
	}

	@Override
	public String getFieldName(RecordScheme original) {
		return original.getField(index);
	}

	@Override
	public OperandRecordField materialize(List<Object> params) {
		return this;
	}

	@Override
	public List<RexDynamicParam> getParams() {
		return Collections.emptyList();
	}

	public int getIndex() {
		return index;
	}

	@Override
	public String toString() {
		return "OperandRecordField[" +
				"index=" + index + ']';
	}

}
