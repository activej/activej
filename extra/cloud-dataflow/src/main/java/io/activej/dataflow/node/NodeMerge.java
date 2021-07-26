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
import io.activej.datastream.processor.StreamReducer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static io.activej.datastream.processor.StreamReducers.deduplicateReducer;
import static io.activej.datastream.processor.StreamReducers.mergeReducer;
import static java.util.Collections.singletonList;

/**
 * Represents a node, which merges many data streams into one, based on a logic, defined by key function and key comparator.
 *
 * @param <K> keys data type
 * @param <T> data items type
 */
public final class NodeMerge<K, T> extends AbstractNode {
	private final Function<T, K> keyFunction;
	private final Comparator<K> keyComparator;
	private final boolean deduplicate;
	private final List<StreamId> inputs;
	private final StreamId output;

	public NodeMerge(int index, Function<T, K> keyFunction, Comparator<K> keyComparator, boolean deduplicate) {
		this(index, keyFunction, keyComparator, deduplicate, new ArrayList<>(), new StreamId());
	}

	public NodeMerge(int index, Function<T, K> keyFunction, Comparator<K> keyComparator, boolean deduplicate, List<StreamId> inputs, StreamId output) {
		super(index);
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.deduplicate = deduplicate;
		this.inputs = inputs;
		this.output = output;
	}

	public void addInput(StreamId input) {
		inputs.add(input);
	}

	@Override
	public List<StreamId> getInputs() {
		return inputs;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return singletonList(output);
	}

	@Override
	public void createAndBind(Task task) {
		StreamReducer<K, T, Void> streamMerger = StreamReducer.create(keyComparator);
		for (StreamId input : inputs) {
			task.bindChannel(input, streamMerger.newInput(keyFunction, deduplicate ? deduplicateReducer() : mergeReducer()));
		}
		task.export(output, streamMerger.getOutput());
	}

	public Function<T, K> getKeyFunction() {
		return keyFunction;
	}

	public Comparator<K> getKeyComparator() {
		return keyComparator;
	}

	public boolean isDeduplicate() {
		return deduplicate;
	}

	public StreamId getOutput() {
		return output;
	}

	@Override
	public String toString() {
		return "NodeMerge{keyFunction=" + keyFunction.getClass().getSimpleName() +
				", keyComparator=" + keyComparator.getClass().getSimpleName() +
				", deduplicate=" + deduplicate +
				", inputs=" + inputs +
				", output=" + output + '}';
	}
}
