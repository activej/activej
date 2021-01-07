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

package io.activej.dataflow.command;

import io.activej.dataflow.graph.TaskStatus;
import io.activej.dataflow.stats.NodeStat;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Map;

public class DataflowResponseTaskData extends DataflowResponse {
	private final TaskStatus status;
	@Nullable
	private final Instant startTime;
	@Nullable
	private final Instant finishTime;
	@Nullable
	private final String err;
	private final Map<Integer, NodeStat> nodes;
	private final String graphViz;

	public DataflowResponseTaskData(TaskStatus status, @Nullable Instant startTime, @Nullable Instant finishTime, @Nullable String err, Map<Integer, NodeStat> nodes, String graphViz) {
		this.status = status;
		this.startTime = startTime;
		this.finishTime = finishTime;
		this.err = err;
		this.nodes = nodes;
		this.graphViz = graphViz;
	}

	public TaskStatus getStatus() {
		return status;
	}

	@Nullable
	public Instant getStartTime() {
		return startTime;
	}

	@Nullable
	public Instant getFinishTime() {
		return finishTime;
	}

	@Nullable
	public String getErrorString() {
		return err;
	}

	public Map<Integer, NodeStat> getNodes() {
		return nodes;
	}

	public String getGraphViz() {
		return graphViz;
	}

	@Override
	public String toString() {
		return "DataflowResponseTaskData{" +
				"status=" + status +
				", startTime=" + startTime +
				", finishTime=" + finishTime +
				", err='" + err + '\'' +
				", nodeStats=" + nodes +
				", graphViz='...'" +
				'}';
	}
}
