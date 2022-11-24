package io.activej.dataflow.http;

import io.activej.dataflow.graph.TaskStatus;
import io.activej.dataflow.stats.NodeStat;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;

public record ReducedTaskData(List<@Nullable TaskStatus> statuses, String graph, Map<Integer, NodeStat> reducedNodeStats) {

	@Override
	public String toString() {
		return "ReducedTaskData{statuses=" + statuses + ", graph='...', reducedNodeStats=" + reducedNodeStats + '}';
	}
}
