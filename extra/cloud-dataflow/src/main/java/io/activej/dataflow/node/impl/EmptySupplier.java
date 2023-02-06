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
import io.activej.datastream.supplier.StreamSuppliers;

import java.util.Collection;
import java.util.List;

/**
 * Represents a node, which produces no items.
 */
@ExposedInternals
public final class EmptySupplier extends AbstractNode {
	public final StreamId output;

	public EmptySupplier(int index, StreamId output) {
		super(index);
		this.output = output;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return List.of(output);
	}

	@Override
	public void createAndBind(Task task) {
		task.export(output, StreamSuppliers.empty());
	}

	@Override
	public String toString() {
		return "SupplierEmpty{output=" + output + '}';
	}
}
