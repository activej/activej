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
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.graph.Task;
import io.activej.dataflow.node.AbstractNode;
import io.activej.dataflow.stats.BinaryNodeStat;
import io.activej.dataflow.stats.NodeStat;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

/**
 * Represents a node, which downloads data from a given address and stream.
 *
 * @param <T> data items type
 */
@ExposedInternals
public final class Download<T> extends AbstractNode {
	public final StreamSchema<T> streamSchema;
	public final InetSocketAddress address;
	public final StreamId streamId;
	public final StreamId output;

	private BinaryNodeStat stats;

	public Download(int index, StreamSchema<T> streamSchema, InetSocketAddress address, StreamId streamId, StreamId output) {
		super(index);
		this.streamSchema = streamSchema;
		this.address = address;
		this.streamId = streamId;
		this.output = output;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return List.of(output);
	}

	@Override
	public void createAndBind(Task task) {
		task.export(output, task.get(DataflowClient.class).download(address, streamId, streamSchema, stats = new BinaryNodeStat()));
	}

	@Override
	public @Nullable NodeStat getStats() {
		return stats;
	}

	@Override
	public String toString() {
		return "Download{type=" + streamSchema + ", address=" + address + ", streamId=" + streamId + ", output=" + output + '}';
	}
}
