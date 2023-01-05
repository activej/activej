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
import io.activej.dataflow.dataset.DatasetUtils;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.node.NodeUpload;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamLimiter;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractCollector<T, A, C extends AbstractCollector<T, A, C>> implements AsyncCollector<T> {
	protected final Dataset<T> input;
	protected final DataflowClient client;

	protected long limit = StreamLimiter.NO_LIMIT;

	protected AbstractCollector(Dataset<T> input, DataflowClient client) {
		this.input = input;
		this.client = client;
	}

	public final C withLimit(long limit) {
		this.limit = limit;
		//noinspection unchecked
		return (C) this;
	}

	protected abstract A createAccumulator();

	protected abstract void accumulate(A accumulator, StreamSupplier<T> supplier);

	protected abstract StreamSupplier<T> getResult(A accumulator);

	@Override
	public final StreamSupplier<T> compile(DataflowGraph graph) {
		DataflowContext context = DataflowContext.of(graph);
		List<StreamId> inputStreamIds = input.channels(context);

		if (limit != StreamLimiter.NO_LIMIT) {
			int index = context.generateNodeIndex();
			List<StreamId> newStreamIds = new ArrayList<>(inputStreamIds.size());
			for (StreamId inputStreamId : inputStreamIds) {
				newStreamIds.add(DatasetUtils.limitStream(graph, index, limit, inputStreamId));
			}
			inputStreamIds = newStreamIds;
		}

		A accumulator = createAccumulator();

		int index = context.generateNodeIndex();
		for (StreamId streamId : inputStreamIds) {
			NodeUpload<T> nodeUpload = new NodeUpload<>(index, input.streamSchema(), streamId);
			Partition partition = graph.getPartition(streamId);
			graph.addNode(partition, nodeUpload);
			StreamSupplier<T> supplier = client.download(partition.getAddress(), streamId, input.streamSchema());
			accumulate(accumulator, supplier);
		}

		//noinspection UnnecessaryLocalVariable
		StreamSupplier<T> result = getResult(accumulator);

		// Code below optimizes limits, but IOException will appear on Dataflow partitions
//		if (limit != StreamLimiter.NO_LIMIT) {
//			result = result.transformWith(StreamLimiter.create(limit));
//		}

		return result;
	}
}
