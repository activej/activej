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
import io.activej.dataflow.graph.TaskContext;
import io.activej.datastream.StreamSupplier;

import java.util.Collection;
import java.util.Iterator;

import static java.util.Collections.singletonList;

/**
 * Represents a node, which produces items as an iterable stream.
 *
 * @param <T> data items type
 */
public final class NodeSupplierOfIterable<T> implements Node {
	private final String iterableId;
	private final StreamId output;

	public NodeSupplierOfIterable(String iterableId) {
		this(iterableId, new StreamId());
	}

	public NodeSupplierOfIterable(String iterableId, StreamId output) {
		this.iterableId = iterableId;
		this.output = output;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return singletonList(output);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void createAndBind(TaskContext taskContext) {
		StreamSupplier<T> supplier;
		Object object = taskContext.get(iterableId);
		if (object instanceof Iterator) {
			supplier = StreamSupplier.ofIterator((Iterator<T>) object);
		} else if (object instanceof Iterable) {
			supplier = StreamSupplier.ofIterable((Iterable<T>) object);
		} else {
			throw new IllegalArgumentException("Object with id '" + iterableId + "' is not an iterator or iterable, it is " + object);
		}
		taskContext.export(output, supplier);
	}

	public Object getIterableId() {
		return iterableId;
	}

	public StreamId getOutput() {
		return output;
	}

	@Override
	public String toString() {
		return "NodeSupplierOfIterable{iterableId=" + iterableId + ", output=" + output + '}';
	}
}
