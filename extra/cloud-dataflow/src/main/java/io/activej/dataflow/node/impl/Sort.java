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
import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.graph.Task;
import io.activej.dataflow.inject.SortingExecutor;
import io.activej.dataflow.node.AbstractNode;
import io.activej.dataflow.node.StreamSorterStorageFactory;
import io.activej.datastream.processor.IStreamSorterStorage;
import io.activej.datastream.processor.StreamSorter;
import io.activej.inject.Key;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Represents a node, which performs sorting of a data stream, based on key function and key comparator.
 *
 * @param <K> keys type
 * @param <T> data items type
 */
@ExposedInternals
public final class Sort<K, T> extends AbstractNode {
	public final StreamSchema<T> streamSchema;
	public final Function<T, K> keyFunction;
	public final Comparator<K> keyComparator;
	public final boolean deduplicate;
	public final int itemsInMemorySize;

	public final StreamId input;
	public final StreamId output;

	public Sort(int index, StreamSchema<T> streamSchema, Function<T, K> keyFunction, Comparator<K> keyComparator,
			boolean deduplicate, int itemsInMemorySize, StreamId input, StreamId output) {
		super(index);
		this.streamSchema = streamSchema;
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.deduplicate = deduplicate;
		this.itemsInMemorySize = itemsInMemorySize;
		this.input = input;
		this.output = output;
	}

	@Override
	public Collection<StreamId> getInputs() {
		return List.of(input);
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return List.of(output);
	}

	@Override
	public void createAndBind(Task task) {
		Executor executor = task.get(Key.of(Executor.class, SortingExecutor.class));
		StreamSorterStorageFactory storageFactory = task.get(StreamSorterStorageFactory.class);
		IStreamSorterStorage<T> storage = storageFactory.create(streamSchema, task, task.getExecutionPromise());
		StreamSorter<K, T> streamSorter = StreamSorter.builder(storage, keyFunction, keyComparator, deduplicate, itemsInMemorySize)
				.withSortingExecutor(executor)
				.build();
		task.bindChannel(input, streamSorter.getInput());
		task.export(output, streamSorter.getOutput());
		streamSorter.getInput().getAcknowledgement()
				.whenComplete(() -> storageFactory.cleanup(storage));
	}

	@Override
	public String toString() {
		return "Sort{type=" + streamSchema +
				", keyFunction=" + keyFunction.getClass().getSimpleName() +
				", keyComparator=" + keyComparator.getClass().getSimpleName() +
				", deduplicate=" + deduplicate +
				", itemsInMemorySize=" + itemsInMemorySize +
				", input=" + input +
				", output=" + output + '}';
	}
}
