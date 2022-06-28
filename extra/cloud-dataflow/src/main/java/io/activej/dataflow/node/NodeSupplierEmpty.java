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

import java.util.Collection;
import java.util.List;

/**
 * Represents a node, which produces no items.
 *
 * @param <T> data items type
 */
public final class NodeSupplierEmpty<T> extends AbstractNode {
	private final StreamId output;

	public NodeSupplierEmpty(int index) {
		this(index, new StreamId());
	}

	public NodeSupplierEmpty(int index, StreamId output) {
		super(index);
		this.output = output;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return List.of(output);
	}

	@Override
	public void createAndBind(Task task) {
		task.export(output, StreamSupplier.of());
	}

	public StreamId getOutput() {
		return output;
	}

	@Override
	public String toString() {
		return "NodeSupplierEmpty{output=" + output + '}';
	}
}
