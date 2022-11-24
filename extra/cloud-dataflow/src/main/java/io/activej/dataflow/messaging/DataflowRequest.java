package io.activej.dataflow.messaging;

import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.node.Node;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public sealed interface DataflowRequest permits
		DataflowRequest.Handshake,
		DataflowRequest.Download,
		DataflowRequest.Execute,
		DataflowRequest.GetTasks {

	record Handshake(Version version) implements DataflowRequest {
	}

	record Download(StreamId streamId) implements DataflowRequest {
	}

	record Execute(long taskId, List<Node> nodes) implements DataflowRequest {
	}

	record GetTasks(@Nullable Long taskId) implements DataflowRequest {
	}
}
