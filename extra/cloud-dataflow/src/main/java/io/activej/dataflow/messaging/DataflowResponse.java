package io.activej.dataflow.messaging;

import io.activej.dataflow.graph.TaskStatus;
import io.activej.dataflow.stats.NodeStat;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public sealed interface DataflowResponse permits
		DataflowResponse.Handshake,
		DataflowResponse.PartitionData,
		DataflowResponse.Result,
		DataflowResponse.TaskData {

	record HandshakeFailure(Version minimalVersion, String message) {
	}

	record Handshake(@Nullable HandshakeFailure handshakeFailure) implements DataflowResponse {
	}

	record Result(@Nullable String error) implements DataflowResponse {
	}

	record TaskDescription(long id, TaskStatus status) {
	}

	record PartitionData(int running, int succeeded, int failed, int cancelled,
						 List<TaskDescription> lastTasks) implements DataflowResponse {
	}

	record TaskData(TaskStatus status, @Nullable Instant startTime, @Nullable Instant finishTime, @Nullable String error,
					Map<Integer, NodeStat> nodes, String graphViz) implements DataflowResponse {
	}
}
