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
import io.activej.common.function.SupplierEx;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.Task;
import io.activej.dataflow.node.AbstractNode;
import io.activej.dataflow.node.PartitionedStreamSupplierFactory;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Represents a node, which produces items as an iterable stream.
 */
@ExposedInternals
public final class SupplierOfId extends AbstractNode {
	public final String id;
	public final int partitionIndex;
	public final int maxPartitions;
	public final StreamId output;

	public SupplierOfId(int index, String id, int partitionIndex, int maxPartitions, StreamId output) {
		super(index);
		this.id = id;
		this.partitionIndex = partitionIndex;
		this.maxPartitions = maxPartitions;
		this.output = output;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return List.of(output);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void createAndBind(Task task) {
		StreamSupplier<?> supplier;
		Object object = task.get(id);
		if (object instanceof Iterator) {
			supplier = StreamSupplier.ofIterator((Iterator<?>) object);
		} else if (object instanceof Iterable) {
			supplier = StreamSupplier.ofIterable((Iterable<?>) object);
		} else if (object instanceof Supplier) {
			supplier = StreamSupplier.ofSupplier(((Supplier<?>) object)::get);
		} else if (object instanceof SupplierEx) {
			supplier = StreamSupplier.ofSupplier((SupplierEx<?>) object);
		} else if (object instanceof Stream) {
			supplier = StreamSupplier.ofStream((Stream<?>) object);
		} else if (object instanceof StreamSupplier) {
			supplier = (StreamSupplier<?>) object;
		} else if (object instanceof ChannelSupplier) {
			supplier = StreamSupplier.ofChannelSupplier((ChannelSupplier<?>) object);
		} else if (object instanceof PartitionedStreamSupplierFactory) {
			supplier = ((PartitionedStreamSupplierFactory<?>) object).get(partitionIndex, maxPartitions);
		} else if (object instanceof Promise) {
			supplier = StreamSupplier.ofPromise(((Promise<StreamSupplier<Object>>) object));
		} else {
			throw new IllegalArgumentException("Object with id '" + id + "' is not a valid supplier of data: " + object);
		}
		task.export(output, supplier);
	}

	@Override
	public String toString() {
		return "SupplierOfId{id='" + id +
				"', partitionIndex=" + partitionIndex +
				", maxPartitions=" + maxPartitions +
				", output=" + output +
				'}';
	}
}
