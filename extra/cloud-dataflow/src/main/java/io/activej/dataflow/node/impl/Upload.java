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

package io.activej.dataflow.node.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.graph.Task;
import io.activej.dataflow.node.AbstractNode;
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
@ExposedInternals
public final class Upload<T> extends AbstractNode {
	public final StreamSchema<T> streamSchema;
	public final StreamId streamId;

	public BinaryNodeStat stats;

	public Upload(int index, StreamSchema<T> streamSchema, StreamId streamId) {
		super(index);
		this.streamSchema = streamSchema;
		this.streamId = streamId;
	}

	@Override
	public Collection<StreamId> getInputs() {
		return List.of(streamId);
	}

	@Override
	public void createAndBind(Task task) {
		task.bindChannel(streamId, task.get(DataflowServer.class).upload(streamId, streamSchema, stats = new BinaryNodeStat()));
	}

	@Override
	public @Nullable NodeStat getStats() {
		return stats;
	}

	@Override
	public String toString() {
		return "Upload{type=" + streamSchema + ", streamId=" + streamId + '}';
	}
}
