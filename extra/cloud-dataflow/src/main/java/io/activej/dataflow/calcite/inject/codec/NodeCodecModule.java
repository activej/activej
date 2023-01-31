package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.node.FilterableSupplierNode;
import io.activej.dataflow.calcite.where.WherePredicate;
import io.activej.dataflow.codec.Subtype;
import io.activej.dataflow.codec.Utils;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;

public final class NodeCodecModule extends AbstractModule {
	@Provides
	@Subtype(15)
	StreamCodec<FilterableSupplierNode<?>> filterableNodeSupplierOfId(
			StreamCodec<WherePredicate> predicateStreamCodec
	) {
		return StreamCodec.create(FilterableSupplierNode::new,
				FilterableSupplierNode::getIndex, StreamCodecs.ofVarInt(),
				FilterableSupplierNode::getId, StreamCodecs.ofString(),
				FilterableSupplierNode::getPredicate, predicateStreamCodec,
				FilterableSupplierNode::getPartitionIndex, StreamCodecs.ofVarInt(),
				FilterableSupplierNode::getMaxPartitions, StreamCodecs.ofVarInt(),
				FilterableSupplierNode::getOutput, Utils.STREAM_ID_STREAM_CODEC
		);
	}
}
