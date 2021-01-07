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
	@Nullable
	private final Instant started;
	@Nullable
	private final Instant finished;
	@Nullable
	private final String error;

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

	@Nullable
	public Instant getStarted() {
		return started;
	}

	@Nullable
	public Instant getFinished() {
		return finished;
	}

	@Nullable
	public String getError() {
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
