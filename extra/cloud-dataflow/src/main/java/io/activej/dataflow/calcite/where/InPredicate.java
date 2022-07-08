package io.activej.dataflow.calcite.where;

import io.activej.record.Record;

import java.util.List;

public final class InPredicate implements WherePredicate {
	private final Operand value;
	private final List<Operand> options;

	public InPredicate(Operand value, List<Operand> options) {
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

	public Operand getValue() {
		return value;
	}

	public List<Operand> getOptions() {
		return options;
	}

	@Override
	public String toString() {
		return "InPredicate[" +
				"value=" + value + ", " +
				"options=" + options + ']';
	}

}
