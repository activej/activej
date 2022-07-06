package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeClass;
import io.activej.serializer.annotations.SerializeNullable;
import org.jetbrains.annotations.Nullable;

public final class OperandScalar implements Operand {
	private final @Nullable Object value;

	public OperandScalar(@Nullable @Deserialize("value") Object value) {
		this.value = value;
	}

	@Override
	public <T> T getValue(Record record) {
		//noinspection unchecked
		return (T) value;
	}

	@Serialize(order = 1)
	@SerializeClass(subclasses = {Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class, Character.class, Boolean.class, String.class})
	@SerializeNullable
	public @Nullable Object getValue() {
		return value;
	}
}
