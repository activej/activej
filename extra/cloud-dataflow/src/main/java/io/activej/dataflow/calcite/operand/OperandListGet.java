package io.activej.dataflow.calcite.operand;

import io.activej.common.Utils;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import org.apache.calcite.rex.RexDynamicParam;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

public final class OperandListGet extends OperandFunction<OperandListGet> {
	private final Operand<?> listOperand;
	private final Operand<?> indexOperand;

	public OperandListGet(Operand<?> listOperand, Operand<?> indexOperand) {
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
	public OperandListGet materialize(List<Object> params) {
		return new OperandListGet(
				listOperand.materialize(params),
				indexOperand.materialize(params)
		);
	}

	@Override
	public List<RexDynamicParam> getParams() {
		return Utils.concat(listOperand.getParams(), indexOperand.getParams());
	}

	public Operand<?> getListOperand() {
		return listOperand;
	}

	public Operand<?> getIndexOperand() {
		return indexOperand;
	}

	@Override
	public List<Operand<?>> getOperands() {
		return List.of(listOperand, indexOperand);
	}

	@Override
	public String toString() {
		return "OperandListGet[" +
				"listOperand=" + listOperand + ", " +
				"indexOperand=" + indexOperand + ']';
	}
}
