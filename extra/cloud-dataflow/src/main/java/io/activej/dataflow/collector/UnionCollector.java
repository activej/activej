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

package io.activej.dataflow.collector;

import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.node.NodeUpload;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamUnion;

import java.util.List;

public class UnionCollector<T> implements Collector<T> {
	private final Dataset<T> input;
	private final DataflowClient client;

	private UnionCollector(Dataset<T> input, DataflowClient client) {
		this.input = input;
		this.client = client;
	}

	public static <T> UnionCollector<T> create(Dataset<T> input, DataflowClient client) {
		return new UnionCollector<>(input, client);
	}

	public StreamSupplier<T> compile(DataflowGraph graph) {
		DataflowContext context = DataflowContext.of(graph);
		List<StreamId> inputStreamIds = input.channels(context);

		StreamUnion<T> union = StreamUnion.create();
		int index = context.generateNodeIndex();
		for (StreamId streamId : inputStreamIds) {
			NodeUpload<T> nodeUpload = new NodeUpload<>(index, input.streamSchema(), streamId);
			Partition partition = graph.getPartition(streamId);
			graph.addNode(partition, nodeUpload);
			StreamSupplier<T> supplier = client.download(partition.getAddress(), streamId, input.streamSchema());
			supplier.streamTo(union.newInput());
		}

		return union.getOutput();
	}
}
