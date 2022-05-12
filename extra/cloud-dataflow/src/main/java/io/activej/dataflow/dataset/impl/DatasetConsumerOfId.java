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

package io.activej.dataflow.dataset.impl;

import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.node.NodeConsumerOfId;

import java.util.List;

public final class DatasetConsumerOfId<T> extends Dataset<T> {
	private final String id;

	private final Dataset<T> input;

	public DatasetConsumerOfId(Dataset<T> input, String id) {
		super(input.valueType());
		this.id = id;
		this.input = input;
	}

	@Override
	public List<StreamId> channels(DataflowContext context) {
		DataflowGraph graph = context.getGraph();
		List<StreamId> streamIds = input.channels(context);
		int index = context.generateNodeIndex();
		for (int i = 0, streamIdsSize = streamIds.size(); i < streamIdsSize; i++) {
			StreamId streamId = streamIds.get(i);
			Partition partition = graph.getPartition(streamId);
			NodeConsumerOfId<T> node = new NodeConsumerOfId<T>(index, id, i, streamIdsSize, streamId);
			graph.addNode(partition, node);
		}
		return List.of();
	}
}
