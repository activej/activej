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

package io.activej.dataflow.calcite.collector;

import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.calcite.utils.EqualObjectComparator;
import io.activej.dataflow.calcite.utils.ToZeroFunction;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.LocallySortedDataset;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.node.NodeUpload;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamReducer;
import io.activej.record.Record;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static io.activej.datastream.processor.StreamReducers.mergeReducer;

public class CalciteCollector<K> {
	private final Dataset<Record> input;
	private final DataflowClient client;
	private final Function<Record, K> keyFunction;
	private final Comparator<K> keyComparator;

	private CalciteCollector(Dataset<Record> input, DataflowClient client,
			Function<Record, K> keyFunction, Comparator<K> keyComparator) {
		this.input = input;
		this.client = client;
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
	}

	public static CalciteCollector<?> create(Dataset<Record> input, DataflowClient client) {
		return new CalciteCollector<>(input, client, ToZeroFunction.getInstance(), EqualObjectComparator.getInstance());
	}

	public static <K> CalciteCollector<K> createOrdered(LocallySortedDataset<K, Record> input, DataflowClient client) {
		return new CalciteCollector<>(input, client, input.keyFunction(), input.keyComparator());
	}

	public StreamSupplier<Record> compile(DataflowGraph graph) {
		DataflowContext context = DataflowContext.of(graph);
		List<StreamId> inputStreamIds = input.channels(context);

		StreamReducer<K, Record, Void> merger = StreamReducer.create(keyComparator);
		int index = context.generateNodeIndex();
		for (StreamId streamId : inputStreamIds) {
			NodeUpload<Record> nodeUpload = new NodeUpload<>(index, input.valueType(), streamId);
			Partition partition = graph.getPartition(streamId);
			graph.addNode(partition, nodeUpload);
			StreamSupplier<Record> supplier = client.download(partition.getAddress(), streamId, input.valueType());
			supplier.streamTo(merger.newInput(keyFunction, mergeReducer()));
		}

		return merger.getOutput();
	}
}
