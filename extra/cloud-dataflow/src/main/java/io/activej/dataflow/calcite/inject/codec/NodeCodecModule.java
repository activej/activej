package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.node.Node_FilterableSupplierOfId;
import io.activej.dataflow.calcite.where.WherePredicate;
import io.activej.dataflow.codec.Subtype;
import io.activej.dataflow.codec.Utils;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;

final class NodeCodecModule extends AbstractModule {
	@Provides
	@Subtype(15)
	StreamCodec<Node_FilterableSupplierOfId<?>> filterableNodeSupplierOfId(
			StreamCodec<WherePredicate> predicateStreamCodec
	) {
		return StreamCodec.create(Node_FilterableSupplierOfId::new,
				Node_FilterableSupplierOfId::getIndex, StreamCodecs.ofVarInt(),
				Node_FilterableSupplierOfId::getId, StreamCodecs.ofString(),
				Node_FilterableSupplierOfId::getPredicate, predicateStreamCodec,
				Node_FilterableSupplierOfId::getPartitionIndex, StreamCodecs.ofVarInt(),
				Node_FilterableSupplierOfId::getMaxPartitions, StreamCodecs.ofVarInt(),
				Node_FilterableSupplierOfId::getOutput, Utils.STREAM_ID_STREAM_CODEC
		);
	}
}
