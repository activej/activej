package io.activej.dataflow.calcite.operand;

import io.activej.common.Utils;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import org.apache.calcite.rex.RexDynamicParam;

import java.lang.reflect.Type;
import java.util.List;

public final class OperandIfNull implements Operand<OperandIfNull> {
	private final Operand<?> checkedOperand;
	private final Operand<?> defaultValueOperand;

	public OperandIfNull(Operand<?> checkedOperand, Operand<?> defaultValueOperand) {
		this.checkedOperand = checkedOperand;
		this.defaultValueOperand = defaultValueOperand;
	}

	@Override
	public <T> T getValue(Record record) {
		Object checked = checkedOperand.getValue(record);
		Object defaultValue = defaultValueOperand.getValue(record);

		//noinspection unchecked
		return (T) (checked == null ? defaultValue : checked);
	}

	@Override
	public Type getFieldType(RecordScheme original) {
		return defaultValueOperand.getFieldType(original);
	}

	@Override
	public String getFieldName(RecordScheme original) {
		String checkedFieldNameName = checkedOperand.getFieldName(original);
		String defaultValueFieldName = defaultValueOperand.getFieldName(original);
		return "IFNULL(" + checkedFieldNameName + ", " + defaultValueFieldName + ")";
	}

	@Override
	public OperandIfNull materialize(List<Object> params) {
		return new OperandIfNull(
				checkedOperand.materialize(params),
				defaultValueOperand.materialize(params)
		);
	}

	@Override
	public List<RexDynamicParam> getParams() {
		return Utils.concat(checkedOperand.getParams(), defaultValueOperand.getParams());
	}

	public Operand<?> getCheckedOperand() {
		return checkedOperand;
	}

	public Operand<?> getDefaultValueOperand() {
		return defaultValueOperand;
	}

	@Override
	public String toString() {
		return "OperandIfNull[" +
				"checkedOperand=" + checkedOperand + ", " +
				"defaultValueOperand=" + defaultValueOperand + ']';
	}

}
