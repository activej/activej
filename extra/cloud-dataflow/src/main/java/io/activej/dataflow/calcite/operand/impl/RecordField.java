package io.activej.dataflow.calcite.operand.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.dataflow.calcite.Param;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.record.Record;
import io.activej.record.RecordScheme;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;

@ExposedInternals
public final class RecordField implements Operand<RecordField> {
	public final int index;

	public RecordField(int index) {
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
	public RecordField materialize(List<Object> params) {
		return this;
	}

	@Override
	public List<Param> getParams() {
		return Collections.emptyList();
	}

	@Override
	public String toString() {
		return "RecordField[index=" + index + ']';
	}
}
