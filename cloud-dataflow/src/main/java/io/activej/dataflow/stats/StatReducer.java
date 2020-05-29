package io.activej.dataflow.stats;

import org.jetbrains.annotations.Nullable;

import java.util.List;

public interface StatReducer<S extends NodeStat> {

	/**
	 * Parameter array can contain nulls
	 */
	NodeStat reduce(List<@Nullable S> statsOnPartitions);
}
