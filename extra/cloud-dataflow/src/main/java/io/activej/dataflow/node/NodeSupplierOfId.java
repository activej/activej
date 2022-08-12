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

import io.activej.common.function.SupplierEx;
import io.activej.csp.ChannelSupplier;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.Task;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Represents a node, which produces items as an iterable stream.
 *
 * @param <T> data items type
 */
public final class NodeSupplierOfId<T> extends AbstractNode {
	private final String id;
	private final int partitionIndex;
	private final int maxPartitions;
	private final StreamId output;

	public NodeSupplierOfId(int index, String id, int partitionIndex, int maxPartitions) {
		this(index, id, partitionIndex, maxPartitions, new StreamId());
	}

	public NodeSupplierOfId(int index, String id, int partitionIndex, int maxPartitions, StreamId output) {
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
		StreamSupplier<T> supplier;
		Object object = task.get(id);
		if (object instanceof Iterator) {
			supplier = StreamSupplier.ofIterator((Iterator<T>) object);
		} else if (object instanceof Iterable) {
			supplier = StreamSupplier.ofIterable((Iterable<T>) object);
		} else if (object instanceof Supplier) {
			supplier = StreamSupplier.ofSupplier(((Supplier<T>) object)::get);
		} else if (object instanceof SupplierEx) {
			supplier = StreamSupplier.ofSupplier((SupplierEx<T>) object);
		} else if (object instanceof Stream) {
			supplier = StreamSupplier.ofStream((Stream<T>) object);
		} else if (object instanceof StreamSupplier) {
			supplier = (StreamSupplier<T>) object;
		} else if (object instanceof ChannelSupplier) {
			supplier = StreamSupplier.ofChannelSupplier((ChannelSupplier<T>) object);
		} else if (object instanceof PartitionedStreamSupplierFactory) {
			supplier = ((PartitionedStreamSupplierFactory<T>) object).get(partitionIndex, maxPartitions);
		} else if (object instanceof Promise) {
			supplier = StreamSupplier.ofPromise(((Promise<StreamSupplier<T>>) object));
		} else {
			throw new IllegalArgumentException("Object with id '" + id + "' is not a valid supplier of data: " + object);
		}
		task.export(output, supplier);
	}

	public String getId() {
		return id;
	}

	public int getPartitionIndex() {
		return partitionIndex;
	}

	public int getMaxPartitions() {
		return maxPartitions;
	}

	public StreamId getOutput() {
		return output;
	}

	@Override
	public String toString() {
		return "NodeSupplierOfId{id='" + id +
				"', partitionIndex=" + partitionIndex +
				", maxPartitions=" + maxPartitions +
				", output=" + output +
				'}';
	}
}
