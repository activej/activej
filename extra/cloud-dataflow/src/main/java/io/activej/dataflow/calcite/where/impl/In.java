package io.activej.dataflow.calcite.where.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.where.WherePredicate;
import io.activej.record.Record;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static io.activej.dataflow.calcite.utils.Utils.equalsUnknown;

@ExposedInternals
public final class In implements WherePredicate {
	public final Operand<?> value;
	public final Collection<Operand<?>> options;

	public In(Operand<?> value, Collection<Operand<?>> options) {
		this.value = value;
		this.options = options;
	}

	@Override
	public boolean test(Record record) {
		Object toTest = value.getValue(record);
		if (toTest == null) return false;

		for (Operand<?> option : options) {
			Object optionValue = option.getValue(record);
			if (optionValue != null && equalsUnknown(toTest, optionValue)) return true;
		}

		return false;
	}

	@Override
	public WherePredicate materialize(List<Object> params) {
		return new In(
				value.materialize(params),
				options.stream()
						.map(option -> option.materialize(params))
						.collect(Collectors.toList())
		);
	}

	@Override
	public String toString() {
		return "In[" +
				"value=" + value + ", " +
				"options=" + options + ']';
	}

}
