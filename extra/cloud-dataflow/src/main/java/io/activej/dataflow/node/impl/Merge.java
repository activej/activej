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
import io.activej.common.builder.AbstractBuilder;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.Task;
import io.activej.dataflow.node.AbstractNode;
import io.activej.datastream.processor.StreamReducer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static io.activej.datastream.processor.reducer.Reducers.deduplicateReducer;
import static io.activej.datastream.processor.reducer.Reducers.mergeReducer;

/**
 * Represents a node, which merges many data streams into one, based on a logic, defined by key function and key comparator.
 *
 * @param <K> keys data type
 * @param <T> data items type
 */
@ExposedInternals
public final class Merge<K, T> extends AbstractNode {
	public final Function<T, K> keyFunction;
	public final Comparator<K> keyComparator;
	public final boolean deduplicate;
	public final List<StreamId> inputs;
	public final StreamId output;

	public Merge(int index, Function<T, K> keyFunction, Comparator<K> keyComparator, boolean deduplicate, List<StreamId> inputs, StreamId output) {
		super(index);
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.deduplicate = deduplicate;
		this.inputs = inputs;
		this.output = output;
	}

	public static <K, T> Merge<K, T>.Builder builder(int index, Function<T, K> keyFunction, Comparator<K> keyComparator, boolean deduplicate) {
		return new Merge<>(index, keyFunction, keyComparator, deduplicate, new ArrayList<>(), new StreamId()).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, Merge<K, T>> {
		private Builder() {}

		public Builder withInput(StreamId input) {
			checkNotBuilt(this);
			inputs.add(input);
			return this;
		}

		@Override
		protected Merge<K, T> doBuild() {
			return Merge.this;
		}
	}

	@Override
	public List<StreamId> getInputs() {
		return inputs;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return List.of(output);
	}

	@Override
	public void createAndBind(Task task) {
		StreamReducer<K, T, Void> streamMerger = StreamReducer.create(keyComparator);
		for (StreamId input : inputs) {
			task.bindChannel(input, streamMerger.newInput(keyFunction, deduplicate ? deduplicateReducer() : mergeReducer()));
		}
		task.export(output, streamMerger.getOutput());
	}

	@Override
	public String toString() {
		return "Merge{keyFunction=" + keyFunction.getClass().getSimpleName() +
				", keyComparator=" + keyComparator.getClass().getSimpleName() +
				", deduplicate=" + deduplicate +
				", inputs=" + inputs +
				", output=" + output + '}';
	}
}
