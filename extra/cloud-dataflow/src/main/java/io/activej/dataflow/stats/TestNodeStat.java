package io.activej.dataflow.stats;

import com.dslplatform.json.CompiledJson;

@CompiledJson
public class TestNodeStat extends NodeStat {

	private final int nodeIndex;

	public TestNodeStat(int nodeIndex) {
		this.nodeIndex = nodeIndex;
	}

	public int getNodeIndex() {
		return nodeIndex;
	}

	@Override
	public String toString() {
		return "TestNodeStats{nodeIndex=" + nodeIndex + '}';
	}
}
