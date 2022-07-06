package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

import java.util.List;

public final class InPredicate implements WherePredicate {
	private final Operand value;
	private final List<Operand> options;

	public InPredicate(@Deserialize("value") Operand value, @Deserialize("options") List<Operand> options) {
		this.value = value;
		this.options = options;
	}

	@Override
	public boolean test(Record record) {
		Object toTest = value.getValue(record);
		if (toTest == null) return false;

		for (Operand option : options) {
			Object optionValue = option.getValue(record);
			if (toTest.equals(optionValue)) return true;
		}

		return false;
	}

	@Serialize(order = 1)
	public Operand getValue() {
		return value;
	}

	@Serialize(order = 2)
	public List<Operand> getOptions() {
		return options;
	}
}
