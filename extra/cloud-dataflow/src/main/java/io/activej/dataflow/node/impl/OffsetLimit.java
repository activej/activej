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
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.Task;
import io.activej.dataflow.node.AbstractNode;
import io.activej.datastream.processor.transformer.StreamTransformer;
import io.activej.datastream.processor.transformer.StreamTransformers;
import io.activej.datastream.processor.transformer.impl.Limiter;
import io.activej.datastream.processor.transformer.impl.Skip;
import io.activej.datastream.supplier.StreamSupplier;

import java.util.Collection;
import java.util.List;

@ExposedInternals
public final class OffsetLimit extends AbstractNode {
	public final StreamId input;
	public final StreamId output;

	public final long offset;
	public final long limit;

	public OffsetLimit(int index, long offset, long limit, StreamId input, StreamId output) {
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
		StreamTransformer<?, ?> skip = StreamTransformers.skip(offset);
		task.bindChannel(input, skip.getInput());

		StreamSupplier<?> supplier = skip.getOutput()
				.transformWith(StreamTransformers.limit(limit));
		task.export(output, supplier);
	}

	@Override
	public List<StreamId> getInputs() {
		return List.of(input);
	}

	@Override
	public String toString() {
		return "OffsetLimit{" +
				"input=" + input +
				", output=" + output +
				", offset=" + (offset == Skip.NO_SKIP ? "NONE" : offset) +
				", limit=" + (limit == Limiter.NO_LIMIT ? "NONE" : limit) +
				'}';
	}
}
