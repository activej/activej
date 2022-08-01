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

import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.Task;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamLimiter;
import io.activej.datastream.processor.StreamSkip;

import java.util.Collection;
import java.util.List;

public final class NodeOffsetLimit<T> extends AbstractNode {
	private final StreamId input;
	private final StreamId output;

	private final long offset;
	private final long limit;

	public NodeOffsetLimit(int index, long offset, long limit, StreamId input) {
		this(index, offset, limit, input, new StreamId());
	}

	public NodeOffsetLimit(int index, long offset, long limit, StreamId input, StreamId output) {
		super(index);
		this.input = input;
		this.output = output;
		this.offset = offset;
		this.limit = limit;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return List.of(output);
	}

	@Override
	public void createAndBind(Task task) {
		StreamSkip<T> skip = StreamSkip.create(offset);
		task.bindChannel(input, skip.getInput());

		StreamSupplier<T> supplier = skip.getOutput()
				.transformWith(StreamLimiter.create(limit));
		task.export(output, supplier);
	}

	@Override
	public List<StreamId> getInputs() {
		return List.of(input);
	}

	public StreamId getInput() {
		return input;
	}

	public StreamId getOutput() {
		return output;
	}

	public long getOffset() {
		return offset;
	}

	public long getLimit() {
		return limit;
	}

	@Override
	public String toString() {
		return "NodeOffsetLimit{" +
				"input=" + input +
				", output=" + output +
				", offset=" + (offset == StreamSkip.NO_SKIP ? "NONE" : offset) +
				", limit=" + (limit == StreamLimiter.NO_LIMIT ? "NONE" : limit) +
				'}';
	}
}
