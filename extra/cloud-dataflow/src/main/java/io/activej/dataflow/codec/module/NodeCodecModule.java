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
public final class NodeCodecModule extends AbstractModule {
	@Override
	protected void configure() {
		install(new ReducerCodecModule());
		install(new StreamSchemaCodecModule());
	}

	@Provides
	@Subtype(0)
	StreamCodec<Node_Reduce> nodeReduce(
			StreamCodec<Comparator> comparatorStreamCodec,
			StreamCodec<Reducer> reducerStreamCodec,
			StreamCodec<Function> functionStreamCodec
	) {
		StreamCodec<Node_Reduce.Input> inputCodec = StreamCodec.create(Node_Reduce.Input::new,
				Node_Reduce.Input::getReducer, reducerStreamCodec,
				Node_Reduce.Input::getKeyFunction, functionStreamCodec
		);
		return StreamCodec.create((a, b, c, d) -> new Node_Reduce(a, b, c, d),
				Node_Reduce::getIndex, StreamCodecs.ofVarInt(),
				Node_Reduce::getKeyComparator, comparatorStreamCodec,
				Node_Reduce::getInputMap, StreamCodecs.ofMap(STREAM_ID_STREAM_CODEC, inputCodec),
				Node_Reduce::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(1)
	StreamCodec<Node_Upload> nodeUpload(
			StreamCodec<StreamSchema> streamSchemaStreamCodec
	) {
		return StreamCodec.create(Node_Upload::new,
				Node_Upload::getIndex, StreamCodecs.ofVarInt(),
				Node_Upload::getStreamSchema, streamSchemaStreamCodec,
				Node_Upload::getStreamId, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(2)
	StreamCodec<Node_OffsetLimit> nodeOffsetLimit() {
		return StreamCodec.create(Node_OffsetLimit::new,
				Node_OffsetLimit::getIndex, StreamCodecs.ofVarInt(),
				Node_OffsetLimit::getOffset, StreamCodecs.ofVarLong(),
				Node_OffsetLimit::getLimit, StreamCodecs.ofVarLong(),
				Node_OffsetLimit::getInput, STREAM_ID_STREAM_CODEC,
				Node_OffsetLimit::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(3)
	StreamCodec<Node_Sort> nodeSort(
			StreamCodec<StreamSchema> streamSchemaStreamCodec,
			StreamCodec<Function> functionStreamCodec,
			StreamCodec<Comparator> comparatorStreamCodec
	) {
		//noinspection unchecked
		return StreamCodec.create(values -> new Node_Sort(
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
						new CodecAndGetter<>(streamSchemaStreamCodec, Node_Sort::getStreamSchema),
						new CodecAndGetter<>(functionStreamCodec, Node_Sort::getKeyFunction),
						new CodecAndGetter<>(comparatorStreamCodec, Node_Sort::getKeyComparator),
						new CodecAndGetter<>(StreamCodecs.ofBoolean(), Node_Sort::isDeduplicate),
						new CodecAndGetter<>(StreamCodecs.ofVarInt(), Node_Sort::getItemsInMemorySize),
						new CodecAndGetter<>(STREAM_ID_STREAM_CODEC, Node_Sort::getInput),
						new CodecAndGetter<>(STREAM_ID_STREAM_CODEC, Node_Sort::getOutput)
				));
	}

	@Provides
	@Subtype(4)
	StreamCodec<Node_Merge> nodeMerge(
			StreamCodec<Function> functionStreamCodec,
			StreamCodec<Comparator> comparatorStreamCodec
	) {
		return StreamCodec.create(Node_Merge::new,
				Node_Merge::getIndex, StreamCodecs.ofVarInt(),
				Node_Merge::getKeyFunction, functionStreamCodec,
				Node_Merge::getKeyComparator, comparatorStreamCodec,
				Node_Merge::isDeduplicate, StreamCodecs.ofBoolean(),
				Node_Merge::getInputs, StreamCodecs.ofList(STREAM_ID_STREAM_CODEC),
				Node_Merge::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(5)
	StreamCodec<Node_Shard> nodeShard(
			StreamCodec<Function> functionStreamCodec
	) {
		return StreamCodec.create(Node_Shard::new,
				Node_Shard::getIndex, StreamCodecs.ofVarInt(),
				Node_Shard::getKeyFunction, functionStreamCodec,
				Node_Shard::getInput, STREAM_ID_STREAM_CODEC,
				Node_Shard::getOutputs, StreamCodecs.ofList(STREAM_ID_STREAM_CODEC),
				Node_Shard::getNonce, StreamCodecs.ofInt()
		);
	}

	@Provides
	@Subtype(6)
	StreamCodec<Node_SupplierEmpty> nodeSupplierEmpty() {
		return StreamCodec.create(Node_SupplierEmpty::new,
				Node_SupplierEmpty::getIndex, StreamCodecs.ofVarInt(),
				Node_SupplierEmpty::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(7)
	StreamCodec<Node_Download> nodeDownload(
			StreamCodec<StreamSchema> streamSchemaStreamCodec
	) {
		return StreamCodec.create(Node_Download::new,
				Node_Download::getIndex, StreamCodecs.ofVarInt(),
				Node_Download::getStreamSchema, streamSchemaStreamCodec,
				Node_Download::getAddress, StreamCodec.create(InetSocketAddress::new,
						InetSocketAddress::getHostName, StreamCodecs.ofString(),
						InetSocketAddress::getPort, StreamCodecs.ofVarInt()),
				Node_Download::getStreamId, STREAM_ID_STREAM_CODEC,
				Node_Download::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(8)
	StreamCodec<Node_Map> nodeMap(
			StreamCodec<Function> functionStreamCodec
	) {
		return StreamCodec.create(Node_Map::new,
				Node_Map::getIndex, StreamCodecs.ofVarInt(),
				Node_Map::getFunction, functionStreamCodec,
				Node_Map::getInput, STREAM_ID_STREAM_CODEC,
				Node_Map::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(9)
	StreamCodec<Node_Join> nodeJoin(
			StreamCodec<Function> functionStreamCodec,
			StreamCodec<Comparator> comparatorStreamCodec,
			StreamCodec<LeftJoiner> leftJoinerStreamCodec
	) {
		return StreamCodec.create(values -> new Node_Join(
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
						new CodecAndGetter<>(StreamCodecs.ofVarInt(), Node_Join::getIndex),
						new CodecAndGetter<>(STREAM_ID_STREAM_CODEC, Node_Join::getLeft),
						new CodecAndGetter<>(STREAM_ID_STREAM_CODEC, Node_Join::getRight),
						new CodecAndGetter<>(STREAM_ID_STREAM_CODEC, Node_Join::getOutput),
						new CodecAndGetter<>(comparatorStreamCodec, Node_Join::getKeyComparator),
						new CodecAndGetter<>(functionStreamCodec, Node_Join::getLeftKeyFunction),
						new CodecAndGetter<>(functionStreamCodec, Node_Join::getRightKeyFunction),
						new CodecAndGetter<>(leftJoinerStreamCodec, Node_Join::getJoiner)
				)
		);
	}

	@Provides
	@Subtype(10)
	StreamCodec<Node_Filter> nodeFilter(
			StreamCodec<Predicate> predicateStreamCodec
	) {
		return StreamCodec.create(Node_Filter::new,
				Node_Filter::getIndex, StreamCodecs.ofVarInt(),
				Node_Filter::getPredicate, predicateStreamCodec,
				Node_Filter::getInput, STREAM_ID_STREAM_CODEC,
				Node_Filter::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(11)
	StreamCodec<Node_ConsumerOfId> nodeConsumerOfId() {
		return StreamCodec.create(Node_ConsumerOfId::new,
				Node_ConsumerOfId::getIndex, StreamCodecs.ofVarInt(),
				Node_ConsumerOfId::getId, StreamCodecs.ofString(),
				Node_ConsumerOfId::getPartitionIndex, StreamCodecs.ofVarInt(),
				Node_ConsumerOfId::getMaxPartitions, StreamCodecs.ofVarInt(),
				Node_ConsumerOfId::getInput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(12)
	StreamCodec<Node_SupplierOfId> nodeSupplierOfId() {
		return StreamCodec.create(Node_SupplierOfId::new,
				Node_SupplierOfId::getIndex, StreamCodecs.ofVarInt(),
				Node_SupplierOfId::getId, StreamCodecs.ofString(),
				Node_SupplierOfId::getPartitionIndex, StreamCodecs.ofVarInt(),
				Node_SupplierOfId::getMaxPartitions, StreamCodecs.ofVarInt(),
				Node_SupplierOfId::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(13)
	StreamCodec<Node_Union> nodeUnion() {
		return StreamCodec.create(Node_Union::new,
				Node_Union::getIndex, StreamCodecs.ofVarInt(),
				Node_Union::getInputs, StreamCodecs.ofList(STREAM_ID_STREAM_CODEC),
				Node_Union::getOutput, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(14)
	StreamCodec<Node_ReduceSimple> nodeReduceSimple(
			StreamCodec<Function> functionStreamCodec,
			StreamCodec<Comparator> comparatorStreamCodec,
			StreamCodec<Reducer> reducerStreamCodec
	) {
		return StreamCodec.create(Node_ReduceSimple::new,
				Node_ReduceSimple::getIndex, StreamCodecs.ofVarInt(),
				Node_ReduceSimple::getKeyFunction, functionStreamCodec,
				Node_ReduceSimple::getKeyComparator, comparatorStreamCodec,
				Node_ReduceSimple::getReducer, reducerStreamCodec,
				Node_ReduceSimple::getInputs, StreamCodecs.ofList(STREAM_ID_STREAM_CODEC),
				Node_ReduceSimple::getOutput, STREAM_ID_STREAM_CODEC
		);
	}
}


