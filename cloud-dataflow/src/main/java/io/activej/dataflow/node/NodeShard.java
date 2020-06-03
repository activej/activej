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
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamSplitter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import static io.activej.common.HashUtils.murmur3hash;
import static io.activej.common.Preconditions.checkArgument;
import static java.util.Collections.singletonList;

/**
 * Represents a node, which splits (duplicates) data items from a single input to many outputs.
 *
 * @param <K> keys type
 * @param <T> data items type
 */
public final class NodeShard<K, T> implements Node {
	private Function<T, K> keyFunction;

	private StreamId input;
	private List<StreamId> outputs;
	private final int nonce;

	public NodeShard(Function<T, K> keyFunction, StreamId input, int nonce) {
		this.keyFunction = keyFunction;
		this.input = input;
		this.outputs = new ArrayList<>();
		this.nonce = nonce;
	}

	public NodeShard(Function<T, K> keyFunction, StreamId input, List<StreamId> outputs, int nonce) {
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

	public void setKeyFunction(Function<T, K> keyFunction) {
		this.keyFunction = keyFunction;
	}

	public StreamId getInput() {
		return input;
	}

	public void setInput(StreamId input) {
		this.input = input;
	}

	@Override
	public Collection<StreamId> getInputs() {
		return singletonList(input);
	}

	@Override
	public List<StreamId> getOutputs() {
		return outputs;
	}

	public void setOutputs(List<StreamId> outputs) {
		this.outputs = outputs;
	}

	static <T> ToIntFunction<T> byHash(int partitions) {
		checkArgument(partitions > 0, "Number of partitions cannot be zero or less");
		int bits = partitions - 1;
		return (partitions & bits) == 0 ?
				object -> object.hashCode() & bits :
				object -> (object.hashCode() & Integer.MAX_VALUE) % partitions;
	}

	public int getNonce() {
		return nonce;
	}

	@Override
	public void createAndBind(TaskContext taskContext) {
		int partitions = outputs.size();
		int bits = partitions - 1;
		BiConsumer<T, StreamDataAcceptor<T>[]> splitter = (partitions & bits) == 0 ?
				(item, acceptors) -> acceptors[murmur3hash(keyFunction.apply(item).hashCode() + nonce) & bits].accept(item) :
				(item, acceptors) -> {
					int hash = murmur3hash(keyFunction.apply(item).hashCode() + nonce);
					int hashAbs = hash < 0 ? hash == Integer.MIN_VALUE ? Integer.MAX_VALUE : -hash : hash;
					acceptors[hashAbs % partitions].accept(item);
				};

		StreamSplitter<T, T> streamSharder = StreamSplitter.create(splitter);

		taskContext.bindChannel(input, streamSharder.getInput());
		for (StreamId streamId : outputs) {
			StreamSupplier<T> supplier = streamSharder.newOutput();
			taskContext.export(streamId, supplier);
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
