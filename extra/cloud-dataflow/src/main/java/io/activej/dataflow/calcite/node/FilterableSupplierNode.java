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

package io.activej.dataflow.calcite.node;

import io.activej.common.function.SupplierEx;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.dataflow.calcite.FilteredDataflowSupplier;
import io.activej.dataflow.calcite.where.WherePredicate;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.Task;
import io.activej.dataflow.node.AbstractNode;
import io.activej.dataflow.node.PartitionedStreamSupplierFactory;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.promise.Promise;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Represents a node, which produces items as an iterable stream with respect to a predicate.
 *
 * @param <T> data items type
 */
public final class FilterableSupplierNode<T> extends AbstractNode {
	private final String id;
	private final WherePredicate predicate;
	private final int partitionIndex;
	private final int maxPartitions;
	private final StreamId output;

	public FilterableSupplierNode(int index, String id, WherePredicate predicate, int partitionIndex, int maxPartitions) {
		this(index, id, predicate, partitionIndex, maxPartitions, new StreamId());
	}

	public FilterableSupplierNode(int index, String id, WherePredicate predicate, int partitionIndex, int maxPartitions, StreamId output) {
		super(index);
		this.id = id;
		this.predicate = predicate;
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
			supplier = StreamSuppliers.ofIterator((Iterator<T>) object);
		} else if (object instanceof Iterable) {
			supplier = StreamSuppliers.ofIterable((Iterable<T>) object);
		} else if (object instanceof Supplier) {
			supplier = StreamSuppliers.ofSupplier(((Supplier<T>) object)::get);
		} else if (object instanceof SupplierEx) {
			supplier = StreamSuppliers.ofSupplier((SupplierEx<T>) object);
		} else if (object instanceof Stream) {
			supplier = StreamSuppliers.ofStream((Stream<T>) object);
		} else if (object instanceof StreamSupplier) {
			supplier = (StreamSupplier<T>) object;
		} else if (object instanceof ChannelSupplier) {
			supplier = StreamSuppliers.ofChannelSupplier((ChannelSupplier<T>) object);
		} else if (object instanceof PartitionedStreamSupplierFactory) {
			supplier = ((PartitionedStreamSupplierFactory<T>) object).get(partitionIndex, maxPartitions);
		} else if (object instanceof Promise) {
			supplier = StreamSuppliers.ofPromise(((Promise<StreamSupplier<T>>) object));
		} else if (object instanceof FilteredDataflowSupplier<?> filteredDataflowSupplier) {
			supplier = (StreamSupplier<T>) filteredDataflowSupplier.create(predicate);
		} else {
			throw new IllegalArgumentException("Object with id '" + id + "' is not a valid supplier of data: " + object);
		}
		task.export(output, supplier);
	}

	public String getId() {
		return id;
	}

	public WherePredicate getPredicate() {
		return predicate;
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
		return "FilterableSupplierNode{id='" + id +
				"', predicate=" + predicate +
				", partitionIndex=" + partitionIndex +
				", maxPartitions=" + maxPartitions +
				", output=" + output +
				'}';
	}
}
