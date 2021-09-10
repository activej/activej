package io.activej.dataflow.http;

import io.activej.dataflow.graph.TaskStatus;
import io.activej.dataflow.stats.NodeStat;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Map;

public final class LocalTaskData {
	private final TaskStatus status;
	private final String graph;
	private final Map<Integer, NodeStat> nodeStats;
	private final @Nullable Instant started;
	private final @Nullable Instant finished;
	private final @Nullable String error;

	public LocalTaskData(TaskStatus status, String graph, Map<Integer, NodeStat> nodeStats, @Nullable Instant started, @Nullable Instant finished, @Nullable String error) {
		this.status = status;
		this.graph = graph;
		this.nodeStats = nodeStats;
		this.started = started;
		this.finished = finished;
		this.error = error;
	}

	public TaskStatus getStatus() {
		return status;
	}

	public String getGraph() {
		return graph;
	}

	public Map<Integer, NodeStat> getNodeStats() {
		return nodeStats;
	}

	public @Nullable Instant getStarted() {
		return started;
	}

	public @Nullable Instant getFinished() {
		return finished;
	}

	public @Nullable String getError() {
		return error;
	}

	@Override
	public String toString() {
		return "LocalTaskData{" +
				"status=" + status +
				", graph='...'" +
				", nodeStats=" + nodeStats +
				", started=" + started +
				", finished=" + finished +
				", error='" + error + '\'' +
				'}';
	}
}
