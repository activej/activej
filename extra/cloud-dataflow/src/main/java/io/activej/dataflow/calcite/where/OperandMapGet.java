package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

import java.util.Map;

public final class OperandMapGet<K, V> implements Operand {
	private final Operand mapOperand;
	private final Operand keyOperand;

	public OperandMapGet(@Deserialize("mapOperand") Operand mapOperand, @Deserialize("keyOperand") Operand keyOperand) {
		this.mapOperand = mapOperand;
		this.keyOperand = keyOperand;
	}

	@Override
	@SuppressWarnings("unchecked")
	public V getValue(Record record) {
		Map<K, V> map = mapOperand.getValue(record);
		K key = keyOperand.getValue(record);

		if (map == null) return null;

		return map.get(key);
	}

	@Serialize(order = 1)
	public Operand getMapOperand() {
		return mapOperand;
	}

	@Serialize(order = 2)
	public Operand getKeyOperand() {
		return keyOperand;
	}
}
