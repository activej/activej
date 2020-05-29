package io.activej.dataflow.stats;

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
