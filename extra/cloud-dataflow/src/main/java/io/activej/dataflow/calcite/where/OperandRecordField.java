package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import io.activej.record.RecordScheme;
import org.apache.calcite.rex.RexDynamicParam;

import java.lang.reflect.Type;
import java.util.List;

public final class OperandRecordField implements Operand {
	private final Operand indexOperand;

	public OperandRecordField(Operand indexOperand) {
		this.indexOperand = indexOperand;
	}

	@Override
	public <T> T getValue(Record record) {
		//noinspection ConstantConditions
		int index = indexOperand.getValue(record);
		return record.get(index);
	}

	@Override
	public Type getFieldType(RecordScheme original) {
		//noinspection ConstantConditions
		return original.getFieldType(((int) indexOperand.getValue(original.record())));
	}

	@Override
	public String getFieldName(RecordScheme original) {
		//noinspection ConstantConditions
		return original.getField(indexOperand.getValue(original.record()));
	}

	@Override
	public Operand materialize(List<Object> params) {
		return new OperandRecordField(indexOperand.materialize(params));
	}

	@Override
	public List<RexDynamicParam> getParams() {
		return indexOperand.getParams();
	}

	public Operand getIndexOperand() {
		return indexOperand;
	}

	@Override
	public String toString() {
		return "OperandRecordField[" +
				"indexOperand=" + indexOperand + ']';
	}

}
