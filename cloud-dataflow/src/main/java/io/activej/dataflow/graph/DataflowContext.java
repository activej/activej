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

package io.activej.dataflow.graph;

import org.jetbrains.annotations.Nullable;

import java.util.concurrent.ThreadLocalRandom;

public final class DataflowContext {
	private final DataflowGraph graph;

	@Nullable
	private final Integer nonce;

	private DataflowContext(DataflowGraph graph, @Nullable Integer nonce) {
		this.nonce = nonce;
		this.graph = graph;
	}

	public static DataflowContext of(DataflowGraph graph) {
		return new DataflowContext(graph, null);
	}

	public DataflowGraph getGraph() {
		return graph;
	}

	public int getNonce() {
		return nonce == null ?
				ThreadLocalRandom.current().nextInt() :
				nonce;
	}

	public DataflowContext withFixedNonce(int nonce) {
		return new DataflowContext(graph, nonce);
	}

	public DataflowContext withoutFixedNonce() {
		return new DataflowContext(graph, null);
	}
}
