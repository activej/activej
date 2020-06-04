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

import io.activej.dataflow.node.Node;

import java.util.List;

public final class DataflowCommandExecute extends DataflowCommand {
	private final List<Node> nodes;

	public DataflowCommandExecute(List<Node> nodes) {
		this.nodes = nodes;
	}

	public List<Node> getNodes() {
		return nodes;
	}

	@Override
	public String toString() {
		return "DatagraphCommandExecute{nodes=" + nodes + "} ";
	}
}
