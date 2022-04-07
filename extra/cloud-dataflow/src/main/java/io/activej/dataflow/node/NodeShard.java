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
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamSplitter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.activej.common.HashUtils.murmur3hash;
import static java.util.Collections.singletonList;

/**
 * Represents a node, which splits (duplicates) data items from a single input to many outputs.
 *
 * @param <K> keys type
 * @param <T> data items type
 */
public final class NodeShard<K, T> extends AbstractNode {
	private final Function<T, K> keyFunction;

	private final int nonce;
	private final StreamId input;
	private final List<StreamId> outputs;

	public NodeShard(int index, Function<T, K> keyFunction, StreamId input, int nonce) {
		this(index, keyFunction, input, new ArrayList<>(), nonce);
	}

	public NodeShard(int index, Function<T, K> keyFunction, StreamId input, List<StreamId> outputs, int nonce) {
		super(index);
		this.keyFunction = keyFunction;
		this.input = input;
		this.outputs = outputs;
		this.nonce = nonce;
	}

	public StreamId newPartition() {
		StreamId newOutput = new StreamId();
		outputs.add(newOutput);
		return newOutput;
	}

	public StreamId getOutput(int partition) {
		return outputs.get(partition);
	}

	public Function<T, K> getKeyFunction() {
		return keyFunction;
	}

	public StreamId getInput() {
		return input;
	}

	@Override
	public Collection<StreamId> getInputs() {
		return singletonList(input);
	}

	@Override
	public List<StreamId> getOutputs() {
		return outputs;
	}

	public int getNonce() {
		return nonce;
	}

	@Override
	public void createAndBind(Task task) {
		int partitions = outputs.size();
		int bits = partitions - 1;
		BiConsumer<T, StreamDataAcceptor<T>[]> splitter = (partitions & bits) == 0 ?
				(item, acceptors) -> {
					acceptors[murmur3hash(keyFunction.apply(item).hashCode() + nonce) & bits].accept(item);
				} :
				(item, acceptors) -> {
					int hash = murmur3hash(keyFunction.apply(item).hashCode() + nonce);
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
		return "NodeShard{keyFunction=" + keyFunction.getClass().getSimpleName() +
				", input=" + input +
				", outputs=" + outputs +
				'}';
	}
}
