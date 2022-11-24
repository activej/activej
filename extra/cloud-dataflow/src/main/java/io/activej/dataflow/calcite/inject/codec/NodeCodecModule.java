package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.node.FilterableNodeSupplierOfId;
import io.activej.dataflow.calcite.where.WherePredicate;
import io.activej.dataflow.codec.StructuredStreamCodec;
import io.activej.dataflow.codec.Subtype;
import io.activej.dataflow.codec.Utils;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;

final class NodeCodecModule extends AbstractModule {
	@Provides
	@Subtype(15)
	StreamCodec<FilterableNodeSupplierOfId<?>> filterableNodeSupplierOfId(
			StreamCodec<WherePredicate> predicateStreamCodec
	) {
		return StructuredStreamCodec.create(FilterableNodeSupplierOfId::new,
				FilterableNodeSupplierOfId::getIndex, StreamCodecs.ofVarInt(),
				FilterableNodeSupplierOfId::getId, StreamCodecs.ofString(),
				FilterableNodeSupplierOfId::getPredicate, predicateStreamCodec,
				FilterableNodeSupplierOfId::getPartitionIndex, StreamCodecs.ofVarInt(),
				FilterableNodeSupplierOfId::getMaxPartitions, StreamCodecs.ofVarInt(),
				FilterableNodeSupplierOfId::getOutput, Utils.STREAM_ID_STREAM_CODEC
		);
	}
}
