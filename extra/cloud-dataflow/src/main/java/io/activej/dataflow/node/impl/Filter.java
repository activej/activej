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
import io.activej.datastream.processor.transformer.StreamTransformer;
import io.activej.datastream.processor.transformer.StreamTransformers;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

/**
 * Represents a node, which filters a data stream and passes to output data items which satisfy a predicate.
 *
 * @param <T> data items type
 */
@ExposedInternals
public final class Filter<T> extends AbstractNode {
	public final Predicate<T> predicate;
	public final StreamId input;
	public final StreamId output;

	public Filter(int index, Predicate<T> predicate, StreamId input, StreamId output) {
		super(index);
		this.predicate = predicate;
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
		StreamTransformer<T, T> streamFilter = StreamTransformers.filter(predicate);
		task.bindChannel(input, streamFilter.getInput());
		task.export(output, streamFilter.getOutput());
	}

	@Override
	public String toString() {
		return "Filter{predicate=" + predicate.getClass().getSimpleName() + ", input=" + input + ", output=" + output + '}';
	}
}
