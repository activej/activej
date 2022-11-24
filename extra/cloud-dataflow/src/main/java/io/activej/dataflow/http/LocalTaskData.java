package io.activej.dataflow.http;

import io.activej.dataflow.graph.TaskStatus;
import io.activej.dataflow.stats.NodeStat;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Map;

public record LocalTaskData(TaskStatus status, String graph, Map<Integer, NodeStat> nodeStats, @Nullable Instant started, @Nullable Instant finished, @Nullable String error) {
}
