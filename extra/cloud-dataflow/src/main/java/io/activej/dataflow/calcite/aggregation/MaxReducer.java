package io.activej.dataflow.calcite.aggregation;

import org.jetbrains.annotations.NotNull;

public final class MaxReducer<I extends Comparable<I>> extends AbstractMinMaxReducer<I> {
	public MaxReducer(int fieldIndex) {
		super(fieldIndex);
	}

	@Override
	public String getName() {
		return "MAX";
	}

	@Override
	protected I compare(@NotNull I current, @NotNull I candidate) {
		if (current.compareTo(candidate) < 0) return candidate;

		return current;
	}
}
