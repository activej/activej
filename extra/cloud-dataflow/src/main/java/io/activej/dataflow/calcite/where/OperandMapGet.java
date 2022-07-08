package io.activej.dataflow.calcite.where;

import io.activej.record.Record;

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

	public Operand getMapOperand() {return mapOperand;}

	public Operand getKeyOperand() {return keyOperand;}

	@Override
	public String toString() {
		return "OperandMapGet[" +
				"mapOperand=" + mapOperand + ", " +
				"keyOperand=" + keyOperand + ']';
	}

}
