package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeClass;
import io.activej.serializer.annotations.SerializeNullable;

public final class OperandScalar<T> implements Operand<T> {
	private final T value;

	public OperandScalar(@Deserialize("value") T value) {
		this.value = value;
	}

	@Override
	public T getValue(Record record) {
		return value;
	}

	@Serialize(order = 1)
	@SerializeClass(subclasses = {Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class, Character.class, Boolean.class, String.class})
	@SerializeNullable
	public Object getValue() {
		return value;
	}
}
