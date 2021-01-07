package io.activej.dataflow.http;

import io.activej.dataflow.graph.TaskStatus;
import io.activej.dataflow.stats.NodeStat;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;

public final class ReducedTaskData {
	private final List<@Nullable TaskStatus> statuses;
	private final String graph;
	private final Map<Integer, @Nullable NodeStat> reducedNodeStats;

	public ReducedTaskData(List<@Nullable TaskStatus> statuses, String graph, Map<Integer, @Nullable NodeStat> reducedNodeStats) {
		this.statuses = statuses;
		this.graph = graph;
		this.reducedNodeStats = reducedNodeStats;
	}

	public List<TaskStatus> getStatuses() {
		return statuses;
	}

	public String getGraph() {
		return graph;
	}

	public Map<Integer, NodeStat> getReducedNodeStats() {
		return reducedNodeStats;
	}

	@Override
	public String toString() {
		return "ReducedTaskData{statuses=" + statuses + ", graph='...', reducedNodeStats=" + reducedNodeStats + '}';
	}
}
