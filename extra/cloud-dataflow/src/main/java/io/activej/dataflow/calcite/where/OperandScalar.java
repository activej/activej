package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import org.jetbrains.annotations.Nullable;

public final class OperandScalar implements Operand {
	private final @Nullable Object value;

	public OperandScalar(@Nullable Object value) {
		this.value = value;
	}

	@Override
	public <T> T getValue(Record record) {
		//noinspection unchecked
		return (T) value;
	}

	public @Nullable Object getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "OperandScalar[" +
				"value=" + value + ']';
	}

}
