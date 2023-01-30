package io.activej.dataflow.codec.module;

import io.activej.dataflow.codec.Subtype;
import io.activej.dataflow.stats.BinaryNodeStat;
import io.activej.dataflow.stats.TestNodeStat;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;

public final class NodeStatCodecModule extends AbstractModule {
	@Provides
	@Subtype(0)
	StreamCodec<BinaryNodeStat> binaryNodeStat() {
		return StreamCodec.create(BinaryNodeStat::new,
				BinaryNodeStat::getBytes, StreamCodecs.ofVarLong()
		);
	}

	@Provides
	@Subtype(1)
	StreamCodec<TestNodeStat> testNodeStat() {
		return StreamCodec.create(TestNodeStat::new,
				TestNodeStat::getNodeIndex, StreamCodecs.ofVarInt()
		);
	}
}
