package io.activej.dataflow.codec.module;

import io.activej.dataflow.codec.Subtype;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.node.*;
import io.activej.datastream.processor.StreamLeftJoin.LeftJoiner;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodec.CodecAndGetter;
import io.activej.serializer.stream.StreamCodecs;

import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.dataflow.codec.Utils.STREAM_ID_STREAM_CODEC;

@SuppressWarnings({"rawtypes", "unchecked"})
final class NodeCodecModule extends AbstractModule {
	@Override
	protected void configure() {
		install(new ReducerCodecModule());
		install(new StreamSchemaCodecModule());
	}

	@Provides
	@Subtype(0)
	StreamCodec<NodeReduce> nodeReduce(
			StreamCodec<Comparator> comparatorStreamCodec,
			StreamCodec<Reducer> reducerStreamCodec,
			StreamCodec<Function> functionStreamCodec
	) {
		StreamCodec<NodeReduce.Input> inputCodec = StreamCodec.create(NodeReduce.Input::new,
				NodeReduce.Input::getReducer, reducerStreamCodec,
				NodeReduce.Input::getKeyFunction, functionStreamCodec
		);
		return StreamCodec.create((a, b, c, d) -> new NodeReduce(a, b, c, d),
				NodeReduce::getIndex, StreamCodecs.ofVarInt(),
				NodeReduce::getKeyComparator, comparatorStreamCodec,
				NodeReduce::getInputMap, StreamCodecs.ofMap(STREAM_ID_STREAM_CODEC, inputCodec),
				NodeReduce::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(1)
	StreamCodec<NodeUpload> nodeUpload(
			StreamCodec<StreamSchema> streamSchemaStreamCodec
	) {
		return StreamCodec.create(NodeUpload::new,
				NodeUpload::getIndex, StreamCodecs.ofVarInt(),
				NodeUpload::getStreamSchema, streamSchemaStreamCodec,
				NodeUpload::getStreamId, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(2)
	StreamCodec<NodeOffsetLimit> nodeOffsetLimit() {
		return StreamCodec.create(NodeOffsetLimit::new,
				NodeOffsetLimit::getIndex, StreamCodecs.ofVarInt(),
				NodeOffsetLimit::getOffset, StreamCodecs.ofVarLong(),
				NodeOffsetLimit::getLimit, StreamCodecs.ofVarLong(),
				NodeOffsetLimit::getInput, STREAM_ID_STREAM_CODEC,
				NodeOffsetLimit::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(3)
	StreamCodec<NodeSort> nodeSort(
			StreamCodec<StreamSchema> streamSchemaStreamCodec,
			StreamCodec<Function> functionStreamCodec,
			StreamCodec<Comparator> comparatorStreamCodec
	) {
		//noinspection unchecked
		return StreamCodec.create(values -> new NodeSort(
						((int) values[0]),
						((StreamSchema<?>) values[1]),
						((Function<?, ?>) values[2]),
						((Comparator<?>) values[3]),
						((boolean) values[4]),
						((int) values[5]),
						((StreamId) values[6]),
						((StreamId) values[7])
				),
				List.of(
						new CodecAndGetter<>(StreamCodecs.ofVarInt(), AbstractNode::getIndex),
						new CodecAndGetter<>(streamSchemaStreamCodec, NodeSort::getStreamSchema),
						new CodecAndGetter<>(functionStreamCodec, NodeSort::getKeyFunction),
						new CodecAndGetter<>(comparatorStreamCodec, NodeSort::getKeyComparator),
						new CodecAndGetter<>(StreamCodecs.ofBoolean(), NodeSort::isDeduplicate),
						new CodecAndGetter<>(StreamCodecs.ofVarInt(), NodeSort::getItemsInMemorySize),
						new CodecAndGetter<>(STREAM_ID_STREAM_CODEC, NodeSort::getInput),
						new CodecAndGetter<>(STREAM_ID_STREAM_CODEC, NodeSort::getOutput)
				));
	}

	@Provides
	@Subtype(4)
	StreamCodec<NodeMerge> nodeMerge(
			StreamCodec<Function> functionStreamCodec,
			StreamCodec<Comparator> comparatorStreamCodec
	) {
		return StreamCodec.create(NodeMerge::new,
				NodeMerge::getIndex, StreamCodecs.ofVarInt(),
				NodeMerge::getKeyFunction, functionStreamCodec,
				NodeMerge::getKeyComparator, comparatorStreamCodec,
				NodeMerge::isDeduplicate, StreamCodecs.ofBoolean(),
				NodeMerge::getInputs, StreamCodecs.ofList(STREAM_ID_STREAM_CODEC),
				NodeMerge::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(5)
	StreamCodec<NodeShard> nodeShard(
			StreamCodec<Function> functionStreamCodec
	) {
		return StreamCodec.create(NodeShard::new,
				NodeShard::getIndex, StreamCodecs.ofVarInt(),
				NodeShard::getKeyFunction, functionStreamCodec,
				NodeShard::getInput, STREAM_ID_STREAM_CODEC,
				NodeShard::getOutputs, StreamCodecs.ofList(STREAM_ID_STREAM_CODEC),
				NodeShard::getNonce, StreamCodecs.ofInt()
		);
	}

	@Provides
	@Subtype(6)
	StreamCodec<NodeSupplierEmpty> nodeSupplierEmpty() {
		return StreamCodec.create(NodeSupplierEmpty::new,
				NodeSupplierEmpty::getIndex, StreamCodecs.ofVarInt(),
				NodeSupplierEmpty::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(7)
	StreamCodec<NodeDownload> nodeDownload(
			StreamCodec<StreamSchema> streamSchemaStreamCodec
	) {
		return StreamCodec.create(NodeDownload::new,
				NodeDownload::getIndex, StreamCodecs.ofVarInt(),
				NodeDownload::getStreamSchema, streamSchemaStreamCodec,
				NodeDownload::getAddress, StreamCodec.create(InetSocketAddress::new,
						InetSocketAddress::getHostName, StreamCodecs.ofString(),
						InetSocketAddress::getPort, StreamCodecs.ofVarInt()),
				NodeDownload::getStreamId, STREAM_ID_STREAM_CODEC,
				NodeDownload::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(8)
	StreamCodec<NodeMap> nodeMap(
			StreamCodec<Function> functionStreamCodec
	) {
		return StreamCodec.create(NodeMap::new,
				NodeMap::getIndex, StreamCodecs.ofVarInt(),
				NodeMap::getFunction, functionStreamCodec,
				NodeMap::getInput, STREAM_ID_STREAM_CODEC,
				NodeMap::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(9)
	StreamCodec<NodeJoin> nodeJoin(
			StreamCodec<Function> functionStreamCodec,
			StreamCodec<Comparator> comparatorStreamCodec,
			StreamCodec<LeftJoiner> leftJoinerStreamCodec
	) {
		return StreamCodec.create(values -> new NodeJoin(
						((int) values[0]),
						((StreamId) values[1]),
						((StreamId) values[2]),
						((StreamId) values[3]),
						((Comparator<?>) values[4]),
						((Function<?, ?>) values[5]),
						((Function<?, ?>) values[6]),
						((LeftJoiner<?, ?, ?, ?>) values[7])
				),
				List.of(
						new CodecAndGetter<>(StreamCodecs.ofVarInt(), NodeJoin::getIndex),
						new CodecAndGetter<>(STREAM_ID_STREAM_CODEC, NodeJoin::getLeft),
						new CodecAndGetter<>(STREAM_ID_STREAM_CODEC, NodeJoin::getRight),
						new CodecAndGetter<>(STREAM_ID_STREAM_CODEC, NodeJoin::getOutput),
						new CodecAndGetter<>(comparatorStreamCodec, NodeJoin::getKeyComparator),
						new CodecAndGetter<>(functionStreamCodec, NodeJoin::getLeftKeyFunction),
						new CodecAndGetter<>(functionStreamCodec, NodeJoin::getRightKeyFunction),
						new CodecAndGetter<>(leftJoinerStreamCodec, NodeJoin::getJoiner)
				)
		);
	}

	@Provides
	@Subtype(10)
	StreamCodec<NodeFilter> nodeFilter(
			StreamCodec<Predicate> predicateStreamCodec
	) {
		return StreamCodec.create(NodeFilter::new,
				NodeFilter::getIndex, StreamCodecs.ofVarInt(),
				NodeFilter::getPredicate, predicateStreamCodec,
				NodeFilter::getInput, STREAM_ID_STREAM_CODEC,
				NodeFilter::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(11)
	StreamCodec<NodeConsumerOfId> nodeConsumerOfId() {
		return StreamCodec.create(NodeConsumerOfId::new,
				NodeConsumerOfId::getIndex, StreamCodecs.ofVarInt(),
				NodeConsumerOfId::getId, StreamCodecs.ofString(),
				NodeConsumerOfId::getPartitionIndex, StreamCodecs.ofVarInt(),
				NodeConsumerOfId::getMaxPartitions, StreamCodecs.ofVarInt(),
				NodeConsumerOfId::getInput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(12)
	StreamCodec<NodeSupplierOfId> nodeSupplierOfId() {
		return StreamCodec.create(NodeSupplierOfId::new,
				NodeSupplierOfId::getIndex, StreamCodecs.ofVarInt(),
				NodeSupplierOfId::getId, StreamCodecs.ofString(),
				NodeSupplierOfId::getPartitionIndex, StreamCodecs.ofVarInt(),
				NodeSupplierOfId::getMaxPartitions, StreamCodecs.ofVarInt(),
				NodeSupplierOfId::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(13)
	StreamCodec<NodeUnion> nodeUnion() {
		return StreamCodec.create(NodeUnion::new,
				NodeUnion::getIndex, StreamCodecs.ofVarInt(),
				NodeUnion::getInputs, StreamCodecs.ofList(STREAM_ID_STREAM_CODEC),
				NodeUnion::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(14)
	StreamCodec<NodeReduceSimple> nodeReduceSimple(
			StreamCodec<Function> functionStreamCodec,
			StreamCodec<Comparator> comparatorStreamCodec,
			StreamCodec<Reducer> reducerStreamCodec
	) {
		return StreamCodec.create(NodeReduceSimple::new,
				NodeReduceSimple::getIndex, StreamCodecs.ofVarInt(),
				NodeReduceSimple::getKeyFunction, functionStreamCodec,
				NodeReduceSimple::getKeyComparator, comparatorStreamCodec,
				NodeReduceSimple::getReducer, reducerStreamCodec,
				NodeReduceSimple::getInputs, StreamCodecs.ofList(STREAM_ID_STREAM_CODEC),
				NodeReduceSimple::getOutput, STREAM_ID_STREAM_CODEC
		);
	}
}


