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
import io.activej.dataflow.stats.NodeStat;
import io.activej.dataflow.stats.TestNodeStat;
import io.activej.datastream.StreamConsumer;

import java.util.Map;

import io.activej.datastream.processor.StreamReducer;
import io.activej.datastream.processor.StreamReducers.Reducer;

import java.util.*;
import java.util.function.Function;

/**
 * Represents a node, which performs 'reduce' operations on a list of input streams, based on a logic, defined by key comparator, key function and reducer for each input.
 *
 * @param <K> keys type
 * @param <O> output data type
 * @param <A> accumulator type
 */
@ExposedInternals
public final class Reduce<K, O, A> extends AbstractNode {
	public record Input<K, O, A>(Reducer<K, ?, O, A> reducer, Function<?, K> keyFunction) {

		@Override
		public String toString() {
			return "Input{reducer=" + reducer.getClass().getSimpleName() +
					", keyFunction=" + keyFunction.getClass().getSimpleName() + '}';
		}
	}

	public final Comparator<K> keyComparator;
	public final Map<StreamId, Input<K, O, A>> inputs;
	public final StreamId output;

	public Reduce(int index, Comparator<K> keyComparator,
			Map<StreamId, Input<K, O, A>> inputs,
			StreamId output) {
		super(index);
		this.keyComparator = keyComparator;
		this.inputs = inputs;
		this.output = output;
	}

	public static <K, O, A> Reduce<K, O, A>.Builder builder(int index, Comparator<K> keyComparator) {
		return new Reduce<K, O, A>(index, keyComparator, new LinkedHashMap<>(), new StreamId()).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, Reduce<K, O, A>> {
		private Builder() {}

		public <I> Builder withInput(StreamId streamId, Function<I, K> keyFunction, Reducer<K, I, O, A> reducer) {
			checkNotBuilt(this);
			inputs.put(streamId, new Input<>(reducer, keyFunction));
			return this;
		}

		@Override
		protected Reduce<K, O, A> doBuild() {
			return Reduce.this;
		}
	}

	@Override
	public Collection<StreamId> getInputs() {
		return inputs.keySet();
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return List.of(output);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void createAndBind(Task task) {
		StreamReducer<K, O, A> streamReducer = StreamReducer.create(keyComparator);
		for (Map.Entry<StreamId, Input<K, O, A>> entry : inputs.entrySet()) {
			Input<K, O, A> koaInput = entry.getValue();
			StreamConsumer<Object> input = streamReducer.newInput(
					((Function<Object, K>) koaInput.keyFunction),
					(Reducer<K, Object, O, A>) koaInput.reducer);
			task.bindChannel(entry.getKey(), input);
		}
		task.export(output, streamReducer.getOutput());
	}

	@Override
	public NodeStat getStats() {
		return new TestNodeStat(getIndex());
	}

	@Override
	public String toString() {
		return "Reduce{keyComparator=" + keyComparator.getClass().getSimpleName() +
				", inputs=" + inputs +
				", output=" + output + '}';
	}
}

