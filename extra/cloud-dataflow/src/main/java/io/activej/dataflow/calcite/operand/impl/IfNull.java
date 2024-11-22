package io.activej.dataflow.calcite.operand.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.common.collection.CollectionUtils;
import io.activej.dataflow.calcite.Param;
import io.activej.dataflow.calcite.operand.FunctionOperand;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.record.Record;
import io.activej.record.RecordScheme;

import java.lang.reflect.Type;
import java.util.List;

@ExposedInternals
public final class IfNull extends FunctionOperand<IfNull> {
	public final Operand<?> checkedOperand;
	public final Operand<?> defaultValueOperand;

	public IfNull(Operand<?> checkedOperand, Operand<?> defaultValueOperand) {
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
	public IfNull materialize(List<Object> params) {
		return new IfNull(
			checkedOperand.materialize(params),
			defaultValueOperand.materialize(params)
		);
	}

	@Override
	public List<Param> getParams() {
		return CollectionUtils.concat(checkedOperand.getParams(), defaultValueOperand.getParams());
	}

	@Override
	public List<Operand<?>> getOperands() {
		return List.of(checkedOperand, defaultValueOperand);
	}

	@Override
	public String toString() {
		return
			"IfNull[" +
			"checkedOperand=" + checkedOperand + ", " +
			"defaultValueOperand=" + defaultValueOperand + ']';
	}

}
