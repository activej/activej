/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.dataflow.node;

import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.Task;
import io.activej.dataflow.stats.BinaryNodeStat;
import io.activej.dataflow.stats.NodeStat;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;

/**
 * Represents a node, which uploads data to a stream.
 *
 * @param <T> data items type
 */
public final class NodeUpload<T> extends AbstractNode {
	private final Class<T> type;
	private final StreamId streamId;

	private BinaryNodeStat stats;

	public NodeUpload(int index, Class<T> type, StreamId streamId) {
		super(index);
		this.type = type;
		this.streamId = streamId;
	}

	@Override
	public Collection<StreamId> getInputs() {
		return List.of(streamId);
	}

	@Override
	public void createAndBind(Task task) {
		task.bindChannel(streamId, task.get(DataflowServer.class).upload(streamId, type, stats = new BinaryNodeStat()));
	}

	public Class<T> getType() {
		return type;
	}

	public StreamId getStreamId() {
		return streamId;
	}

	@Override
	public @Nullable NodeStat getStats() {
		return stats;
	}

	@Override
	public String toString() {
		return "NodeUpload{type=" + type + ", streamId=" + streamId + '}';
	}
}
