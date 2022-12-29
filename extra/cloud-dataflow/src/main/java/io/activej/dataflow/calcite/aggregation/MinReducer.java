package io.activej.dataflow.calcite.aggregation;

import org.jetbrains.annotations.Nullable;

public final class MinReducer<I extends Comparable<I>> extends AbstractMinMaxReducer<I> {
	public MinReducer(int fieldIndex, @Nullable String fieldAlias) {
		super(fieldIndex, fieldAlias);
	}

	@Override
	public String doGetName(String fieldName) {
		return "MIN(" + fieldName + ')';
	}

	@Override
	protected I compare(I current, I candidate) {
		if (current.compareTo(candidate) > 0) return candidate;

		return current;
	}
}
