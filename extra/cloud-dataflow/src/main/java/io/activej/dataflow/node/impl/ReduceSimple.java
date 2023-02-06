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
import io.activej.datastream.processor.reducer.Reducer;
import io.activej.datastream.processor.reducer.StreamReducer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

/**
 * Represents a simple reducer node, with a single key function and reducer for all internalConsumers.
 *
 * @param <K> keys type
 * @param <I> input data type
 * @param <O> output data type
 * @param <A> accumulator type
 */
@ExposedInternals
public final class ReduceSimple<K, I, O, A> extends AbstractNode {
	public final Function<I, K> keyFunction;
	public final Comparator<K> keyComparator;
	public final Reducer<K, I, O, A> reducer;
	public final List<StreamId> inputs;
	public final StreamId output;

	public ReduceSimple(int index, Function<I, K> keyFunction, Comparator<K> keyComparator, Reducer<K, I, O, A> reducer,
			List<StreamId> inputs, StreamId output) {
		super(index);
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.reducer = reducer;
		this.inputs = inputs;
		this.output = output;
	}

	public static <K, I, O, A> ReduceSimple<K, I, O, A>.Builder builder(int index, Function<I, K> keyFunction,
			Comparator<K> keyComparator, Reducer<K, I, O, A> reducer) {
		return new ReduceSimple<>(index, keyFunction, keyComparator, reducer, new ArrayList<>(), new StreamId()).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, ReduceSimple<K, I, O, A>> {
		private Builder() {}

		public Builder withInput(StreamId input) {
			checkNotBuilt(this);
			inputs.add(input);
			return this;
		}

		@Override
		protected ReduceSimple<K, I, O, A> doBuild() {
			return ReduceSimple.this;
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
		StreamReducer<K, O, A> streamReducerSimple = StreamReducer.create(keyComparator);
		for (StreamId input : inputs) {
			task.bindChannel(input, streamReducerSimple.newInput(keyFunction, reducer));
		}
		task.export(output, streamReducerSimple.getOutput());
	}

	@Override
	public String toString() {
		return "ReduceSimple{keyFunction=" + keyFunction.getClass().getSimpleName() +
				", keyComparator=" + keyComparator.getClass().getSimpleName() +
				", reducer=" + reducer.getClass().getSimpleName() +
				", inputs=" + inputs +
				", output=" + output + '}';
	}
}

