package io.activej.dataflow.calcite.where;

import io.activej.dataflow.calcite.Value;
import io.activej.record.Record;
import org.apache.calcite.rex.RexDynamicParam;

import java.util.Collections;
import java.util.List;

public final class OperandScalar implements Operand {

	private final Value value;

	public OperandScalar(Value value) {
		this.value = value;
	}

	@Override
	public <T> T getValue(Record record) {
		//noinspection unchecked
		return (T) value.getValue();
	}

	@Override
	public Operand materialize(List<Object> params) {
		return new OperandScalar(value.materialize(params));
	}

	@Override
	public List<RexDynamicParam> getParams() {
		return value.isMaterialized() ?
				Collections.emptyList() :
				Collections.singletonList(value.getDynamicParam());
	}

	public Value getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "OperandScalar[" +
				"value=" + value + ']';
	}

}
