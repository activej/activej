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

import io.activej.common.annotation.ExposedInternals;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.StreamId;
import io.activej.datastream.processor.StreamLimiter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.activej.dataflow.dataset.DatasetUtils.limitStream;

@ExposedInternals
public final class LocalLimit<T> extends Dataset<T> {
	public final Dataset<T> input;
	public final long limit;

	public LocalLimit(Dataset<T> input, long limit) {
		super(input.streamSchema());
		this.input = input;
		this.limit = limit;
	}

	@Override
	public List<StreamId> channels(DataflowContext context) {
		List<StreamId> streamIds = input.channels(context);

		if (limit == StreamLimiter.NO_LIMIT) return streamIds;

		DataflowGraph graph = context.getGraph();

		if (streamIds.isEmpty()) return streamIds;

		List<StreamId> newStreamIds = new ArrayList<>(streamIds.size());
		for (StreamId streamId : streamIds) {
			newStreamIds.addAll(limitStream(graph, context.generateNodeIndex(), limit, streamId));
		}

		return newStreamIds;
	}

	@Override
	public Collection<Dataset<?>> getBases() {
		return List.of(input);
	}
}
