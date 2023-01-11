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
import io.activej.datastream.processor.StreamLeftJoin;
import io.activej.datastream.processor.StreamLeftJoin.LeftJoiner;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

/**
 * Represents a node, which joins two internalConsumers streams (left and right) into one, based on logic, defined by key functions and joiner.
 *
 * @param <K> keys type
 * @param <L> left stream data type
 * @param <R> right stream data type
 * @param <V> output stream data type
 */
public final class Node_Join<K, L, R, V> extends AbstractNode {
	private final StreamId left;
	private final StreamId right;
	private final StreamId output;
	private final Comparator<K> keyComparator;
	private final Function<L, K> leftKeyFunction;
	private final Function<R, K> rightKeyFunction;
	private final LeftJoiner<K, L, R, V> leftJoiner;

	public Node_Join(int index, StreamId left, StreamId right,
			Comparator<K> keyComparator, Function<L, K> leftKeyFunction, Function<R, K> rightKeyFunction,
			LeftJoiner<K, L, R, V> leftJoiner) {
		this(index, left, right, new StreamId(), keyComparator, leftKeyFunction, rightKeyFunction, leftJoiner);
	}

	public Node_Join(int index, StreamId left, StreamId right, StreamId output,
			Comparator<K> keyComparator, Function<L, K> leftKeyFunction, Function<R, K> rightKeyFunction,
			LeftJoiner<K, L, R, V> leftJoiner) {
		super(index);
		this.left = left;
		this.right = right;
		this.output = output;
		this.keyComparator = keyComparator;
		this.leftKeyFunction = leftKeyFunction;
		this.rightKeyFunction = rightKeyFunction;
		this.leftJoiner = leftJoiner;
	}

	@Override
	public Collection<StreamId> getInputs() {
		return List.of(left, right);
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return List.of(output);
	}

	@Override
	public void createAndBind(Task task) {
		StreamLeftJoin<K, L, R, V> join = StreamLeftJoin.create(keyComparator, leftKeyFunction, rightKeyFunction, leftJoiner);
		task.bindChannel(left, join.getLeft());
		task.bindChannel(right, join.getRight());
		task.export(output, join.getOutput());
	}

	public StreamId getLeft() {
		return left;
	}

	public StreamId getRight() {
		return right;
	}

	public StreamId getOutput() {
		return output;
	}

	public Comparator<K> getKeyComparator() {
		return keyComparator;
	}

	public Function<L, K> getLeftKeyFunction() {
		return leftKeyFunction;
	}

	public Function<R, K> getRightKeyFunction() {
		return rightKeyFunction;
	}

	public LeftJoiner<K, L, R, V> getJoiner() {
		return leftJoiner;
	}

	@Override
	public String toString() {
		return "NodeJoin{left=" + left +
				", right=" + right +
				", output=" + output +
				", keyComparator=" + keyComparator.getClass().getSimpleName() +
				", leftKeyFunction=" + leftKeyFunction.getClass().getSimpleName() +
				", rightKeyFunction=" + rightKeyFunction.getClass().getSimpleName() +
				", joiner=" + leftJoiner.getClass().getSimpleName() + '}';
	}
}
