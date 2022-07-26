package io.activej.dataflow.calcite.where;

import io.activej.common.Utils;
import io.activej.record.Record;
import org.apache.calcite.rex.RexDynamicParam;

import java.util.List;
import java.util.Map;

public final class OperandMapGet<K> implements Operand {
	private final Operand mapOperand;
	private final Operand keyOperand;

	public OperandMapGet(Operand mapOperand, Operand keyOperand) {
		this.mapOperand = mapOperand;
		this.keyOperand = keyOperand;
	}

	@Override
	public <V> V getValue(Record record) {
		Map<K, V> map = mapOperand.getValue(record);
		K key = keyOperand.getValue(record);

		if (map == null) return null;

		return map.get(key);
	}

	@Override
	public Operand materialize(List<Object> params) {
		return new OperandMapGet<>(
				mapOperand.materialize(params),
				keyOperand.materialize(params)
		);
	}

	@Override
	public List<RexDynamicParam> getParams() {
		return Utils.concat(mapOperand.getParams(), keyOperand.getParams());
	}

	public Operand getMapOperand() {
		return mapOperand;
	}

	public Operand getKeyOperand() {
		return keyOperand;
	}

	@Override
	public String toString() {
		return "OperandMapGet[" +
				"mapOperand=" + mapOperand + ", " +
				"keyOperand=" + keyOperand + ']';
	}

}
