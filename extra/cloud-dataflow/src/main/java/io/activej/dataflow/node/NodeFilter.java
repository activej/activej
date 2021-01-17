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
import io.activej.datastream.processor.StreamFilter;

import java.util.Collection;
import java.util.function.Predicate;

import static java.util.Collections.singletonList;

/**
 * Represents a node, which filters a data stream and passes to output data items which satisfy a predicate.
 *
 * @param <T> data items type
 */
public final class NodeFilter<T> extends AbstractNode {
	private final Predicate<T> predicate;
	private final StreamId input;
	private final StreamId output;

	public NodeFilter(int index, Predicate<T> predicate, StreamId input) {
		this(index, predicate, input, new StreamId());
	}

	public NodeFilter(int index, Predicate<T> predicate, StreamId input, StreamId output) {
		super(index);
		this.predicate = predicate;
		this.input = input;
		this.output = output;
	}

	@Override
	public Collection<StreamId> getInputs() {
		return singletonList(input);
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return singletonList(output);
	}

	@Override
	public void createAndBind(Task task) {
		StreamFilter<T, T> streamFilter = StreamFilter.create(predicate);
		task.bindChannel(input, streamFilter.getInput());
		task.export(output, streamFilter.getOutput());
	}

	public Predicate<T> getPredicate() {
		return predicate;
	}

	public StreamId getInput() {
		return input;
	}

	public StreamId getOutput() {
		return output;
	}

	@Override
	public String toString() {
		return "NodeFilter{predicate=" + predicate.getClass().getSimpleName() + ", input=" + input + ", output=" + output + '}';
	}
}
