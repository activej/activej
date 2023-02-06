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
import io.activej.datastream.processor.StreamSplitter;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.datastream.supplier.StreamSupplier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.activej.common.HashUtils.murmur3hash;

/**
 * Represents a node, which splits (duplicates) data items from a single input to many outputs.
 *
 * @param <K> keys type
 * @param <T> data items type
 */
@ExposedInternals
public final class Shard<K, T> extends AbstractNode {
	public final Function<T, K> keyFunction;

	public final int nonce;
	public final StreamId input;
	public final List<StreamId> outputs;

	public Shard(int index, Function<T, K> keyFunction, StreamId input, List<StreamId> outputs, int nonce) {
		super(index);
		this.keyFunction = keyFunction;
		this.input = input;
		this.outputs = outputs;
		this.nonce = nonce;
	}

	public static <K, T> Shard<K, T> create(int index, Function<T, K> keyFunction, StreamId input, int nonce) {
		return new Shard<>(index, keyFunction, input, new ArrayList<>(), nonce);
	}

	public StreamId newPartition() {
		StreamId newOutput = new StreamId();
		outputs.add(newOutput);
		return newOutput;
	}

	public StreamId getOutput(int partition) {
		return outputs.get(partition);
	}

	@Override
	public Collection<StreamId> getInputs() {
		return List.of(input);
	}

	@Override
	public List<StreamId> getOutputs() {
		return outputs;
	}

	@Override
	public void createAndBind(Task task) {
		int partitions = outputs.size();
		int bits = partitions - 1;
		BiConsumer<T, StreamDataAcceptor<T>[]> splitter = (partitions & bits) == 0 ?
				(item, acceptors) -> acceptors[murmur3hash(Objects.hashCode(keyFunction.apply(item)) + nonce) & bits].accept(item) :
				(item, acceptors) -> {
					int hash = murmur3hash(Objects.hashCode(keyFunction.apply(item)) + nonce);
					int hashAbs = hash < 0 ? hash == Integer.MIN_VALUE ? Integer.MAX_VALUE : -hash : hash;
					acceptors[hashAbs % partitions].accept(item);
				};

		StreamSplitter<T, T> streamSharder = StreamSplitter.create(splitter);

		task.bindChannel(input, streamSharder.getInput());
		for (StreamId streamId : outputs) {
			StreamSupplier<T> supplier = streamSharder.newOutput();
			task.export(streamId, supplier);
		}
	}

	@Override
	public String toString() {
		return "Shard{keyFunction=" + keyFunction.getClass().getSimpleName() +
				", input=" + input +
				", outputs=" + outputs +
				'}';
	}
}
