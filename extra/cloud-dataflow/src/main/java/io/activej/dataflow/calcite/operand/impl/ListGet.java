package io.activej.dataflow.calcite.operand.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.common.collection.CollectionUtils;
import io.activej.dataflow.calcite.Param;
import io.activej.dataflow.calcite.operand.FunctionOperand;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.record.Record;
import io.activej.record.RecordScheme;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

@ExposedInternals
public final class ListGet extends FunctionOperand<ListGet> {
	public final Operand<?> listOperand;
	public final Operand<?> indexOperand;

	public ListGet(Operand<?> listOperand, Operand<?> indexOperand) {
		this.listOperand = listOperand;
		this.indexOperand = indexOperand;
	}

	@Override
	public <T> T getValue(Record record) {
		List<T> list = listOperand.getValue(record);
		Number index = indexOperand.getValue(record);

		if (list == null || index == null || list.size() <= index.intValue()) return null;

		return list.get(index.intValue());
	}

	@Override
	public Type getFieldType(RecordScheme original) {
		Type listFieldType = listOperand.getFieldType(original);
		return ((ParameterizedType) listFieldType).getActualTypeArguments()[0];
	}

	@Override
	public String getFieldName(RecordScheme original) {
		String listFieldName = listOperand.getFieldName(original);
		String indexFieldName = indexOperand.getFieldName(original);
		return listFieldName + ".get(" + indexFieldName + ")";
	}

	@Override
	public ListGet materialize(List<Object> params) {
		return new ListGet(
			listOperand.materialize(params),
			indexOperand.materialize(params)
		);
	}

	@Override
	public List<Param> getParams() {
		return CollectionUtils.concat(listOperand.getParams(), indexOperand.getParams());
	}

	@Override
	public List<Operand<?>> getOperands() {
		return List.of(listOperand, indexOperand);
	}

	@Override
	public String toString() {
		return
			"ListGet[" +
			"listOperand=" + listOperand + ", " +
			"indexOperand=" + indexOperand + ']';
	}
}
