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

import io.activej.common.ref.RefInt;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.ThreadLocalRandom;

public final class DataflowContext {
	private final DataflowGraph graph;

	private final RefInt nextNodeIndex;

	private final @Nullable Integer nonce;

	private DataflowContext(DataflowGraph graph, @Nullable Integer nonce, RefInt nextNodeIndex) {
		this.nonce = nonce;
		this.graph = graph;
		this.nextNodeIndex = nextNodeIndex;
	}

	public static DataflowContext of(DataflowGraph graph) {
		return new DataflowContext(graph, null, new RefInt(0));
	}

	public int generateNodeIndex() {
		return nextNodeIndex.value++;
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
		return new DataflowContext(graph, nonce, nextNodeIndex);
	}

	public DataflowContext withoutFixedNonce() {
		return new DataflowContext(graph, null, nextNodeIndex);
	}
}
