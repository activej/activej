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
import io.activej.datastream.processor.StreamUnion;

import java.util.Collection;
import java.util.List;

@ExposedInternals
public final class Union extends AbstractNode {
	public final List<StreamId> inputs;
	public final StreamId output;

	public Union(int index, List<StreamId> inputs, StreamId output) {
		super(index);
		this.inputs = inputs;
		this.output = output;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return List.of(output);
	}

	@Override
	public void createAndBind(Task task) {
		StreamUnion<?> streamUnion = StreamUnion.create();
		for (StreamId input : inputs) {
			task.bindChannel(input, streamUnion.newInput());
		}
		task.export(output, streamUnion.getOutput());
	}

	@Override
	public List<StreamId> getInputs() {
		return inputs;
	}

	@Override
	public String toString() {
		return "Union{inputs=" + inputs + ", output=" + output + '}';
	}
}
