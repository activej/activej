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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.Task;
import io.activej.dataflow.stats.NodeStat;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

/**
 * Defines a node in a single server.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
public interface Node {
	/**
	 * Returns an index of this node in the task.
	 * When the graph is spread over the cluster, nodes with the
	 * same index on multiple partitions are the ones that were
	 * compiled from the same dataset.
	 *
	 * @return index of the node that is unique in the partition scope
	 */
	int getIndex();

	/**
	 * Returns a list of ids of inputs of this node.
	 *
	 * @return ids of inputs of this node
	 */
	default Collection<StreamId> getInputs() {
		return List.of();
	}

	/**
	 * Returns a list of ids of outputs of this node.
	 *
	 * @return ids of outputs of this node
	 */
	default Collection<StreamId> getOutputs() {
		return List.of();
	}

	/**
	 * Defines internal node logic and binds it to the task context.
	 *
	 * @param task task context to which certain logic is to be bound
	 */
	void createAndBind(Task task);

	/**
	 * Is called when all streams that were exported by this node finish streaming.
	 */
	void finish(@Nullable Exception error);

	/**
	 * Returns a point in time when this node instance finished processing data.
	 * Must return null if node did not finish processing yet.
	 */
	@Nullable Instant getFinished();

	/**
	 * If this node caused the task to fail, returns the exception that caused it.
	 */
	@Nullable Exception getError();

	/**
	 * Optionally return some custom node statistics for debugging.
	 */
	@Nullable NodeStat getStats();
}
