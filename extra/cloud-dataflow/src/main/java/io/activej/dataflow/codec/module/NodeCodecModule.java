package io.activej.dataflow.codec.module;

import io.activej.dataflow.codec.Subtype;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.node.*;
import io.activej.dataflow.node.impl.*;
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
	StreamCodec<Reduce> nodeReduce(
			StreamCodec<Comparator> comparatorStreamCodec,
			StreamCodec<Reducer> reducerStreamCodec,
			StreamCodec<Function> functionStreamCodec
	) {
		StreamCodec<Reduce.Input> inputCodec = StreamCodec.create(Reduce.Input::new,
				Reduce.Input::getReducer, reducerStreamCodec,
				Reduce.Input::getKeyFunction, functionStreamCodec
		);
		return StreamCodec.create((a, b, c, d) -> new Reduce(a, b, c, d),
				Reduce::getIndex, StreamCodecs.ofVarInt(),
				reduce -> reduce.keyComparator, comparatorStreamCodec,
				reduce -> reduce.inputs, StreamCodecs.ofMap(STREAM_ID_STREAM_CODEC, inputCodec),
				reduce -> reduce.output, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(1)
	StreamCodec<Upload> nodeUpload(
			StreamCodec<StreamSchema> streamSchemaStreamCodec
	) {
		return StreamCodec.create(Upload::new,
				Upload::getIndex, StreamCodecs.ofVarInt(),
				upload -> upload.streamSchema, streamSchemaStreamCodec,
				upload -> upload.streamId, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(2)
	StreamCodec<OffsetLimit> nodeOffsetLimit() {
		return StreamCodec.create(OffsetLimit::new,
				OffsetLimit::getIndex, StreamCodecs.ofVarInt(),
				offsetLimit -> offsetLimit.offset, StreamCodecs.ofVarLong(),
				offsetLimit -> offsetLimit.limit, StreamCodecs.ofVarLong(),
				offsetLimit -> offsetLimit.input, STREAM_ID_STREAM_CODEC,
				offsetLimit -> offsetLimit.output, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(3)
	StreamCodec<Sort> nodeSort(
			StreamCodec<StreamSchema> streamSchemaStreamCodec,
			StreamCodec<Function> functionStreamCodec,
			StreamCodec<Comparator> comparatorStreamCodec
	) {
		//noinspection unchecked
		return StreamCodec.create(values -> new Sort(
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
						new CodecAndGetter<>(streamSchemaStreamCodec, sort -> sort.streamSchema),
						new CodecAndGetter<>(functionStreamCodec, sort -> sort.keyFunction),
						new CodecAndGetter<>(comparatorStreamCodec, sort -> sort.keyComparator),
						new CodecAndGetter<>(StreamCodecs.ofBoolean(), sort -> sort.deduplicate),
						new CodecAndGetter<>(StreamCodecs.ofVarInt(), sort -> sort.itemsInMemorySize),
						new CodecAndGetter<>(STREAM_ID_STREAM_CODEC, sort -> sort.input),
						new CodecAndGetter<>(STREAM_ID_STREAM_CODEC, sort -> sort.output)
				));
	}

	@Provides
	@Subtype(4)
	StreamCodec<Merge> nodeMerge(
			StreamCodec<Function> functionStreamCodec,
			StreamCodec<Comparator> comparatorStreamCodec
	) {
		return StreamCodec.create(Merge::new,
				Merge::getIndex, StreamCodecs.ofVarInt(),
				merge -> merge.keyFunction, functionStreamCodec,
				merge -> merge.keyComparator, comparatorStreamCodec,
				merge -> merge.deduplicate, StreamCodecs.ofBoolean(),
				Merge::getInputs, StreamCodecs.ofList(STREAM_ID_STREAM_CODEC),
				merge -> merge.output, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(5)
	StreamCodec<Shard> nodeShard(
			StreamCodec<Function> functionStreamCodec
	) {
		return StreamCodec.create(Shard::new,
				Shard::getIndex, StreamCodecs.ofVarInt(),
				shard -> shard.keyFunction, functionStreamCodec,
				shard -> shard.input, STREAM_ID_STREAM_CODEC,
				Shard::getOutputs, StreamCodecs.ofList(STREAM_ID_STREAM_CODEC),
				shard -> shard.nonce, StreamCodecs.ofInt()
		);
	}

	@Provides
	@Subtype(6)
	StreamCodec<EmptySupplier> nodeSupplierEmpty() {
		return StreamCodec.create(EmptySupplier::new,
				EmptySupplier::getIndex, StreamCodecs.ofVarInt(),
				emptySupplier -> emptySupplier.output, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(7)
	StreamCodec<Download> nodeDownload(
			StreamCodec<StreamSchema> streamSchemaStreamCodec
	) {
		return StreamCodec.create(Download::new,
				Download::getIndex, StreamCodecs.ofVarInt(),
				download -> download.streamSchema, streamSchemaStreamCodec,
				download -> download.address, StreamCodec.create(InetSocketAddress::new,
						InetSocketAddress::getHostName, StreamCodecs.ofString(),
						InetSocketAddress::getPort, StreamCodecs.ofVarInt()),
				download -> download.streamId, STREAM_ID_STREAM_CODEC,
				download -> download.output, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(8)
	StreamCodec<Map> nodeMap(
			StreamCodec<Function> functionStreamCodec
	) {
		return StreamCodec.create(Map::new,
				Map::getIndex, StreamCodecs.ofVarInt(),
				map -> map.function, functionStreamCodec,
				map -> map.input, STREAM_ID_STREAM_CODEC,
				map -> map.output, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(9)
	StreamCodec<Join> nodeJoin(
			StreamCodec<Function> functionStreamCodec,
			StreamCodec<Comparator> comparatorStreamCodec,
			StreamCodec<LeftJoiner> leftJoinerStreamCodec
	) {
		return StreamCodec.create(values -> new Join(
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
						new CodecAndGetter<>(StreamCodecs.ofVarInt(), Join::getIndex),
						new CodecAndGetter<>(STREAM_ID_STREAM_CODEC, join -> join.left),
						new CodecAndGetter<>(STREAM_ID_STREAM_CODEC, join -> join.right),
						new CodecAndGetter<>(STREAM_ID_STREAM_CODEC, join -> join.output),
						new CodecAndGetter<>(comparatorStreamCodec, join -> join.keyComparator),
						new CodecAndGetter<>(functionStreamCodec, join -> join.leftKeyFunction),
						new CodecAndGetter<>(functionStreamCodec, join -> join.rightKeyFunction),
						new CodecAndGetter<>(leftJoinerStreamCodec, join -> join.leftJoiner)
				)
		);
	}

	@Provides
	@Subtype(10)
	StreamCodec<Filter> nodeFilter(
			StreamCodec<Predicate> predicateStreamCodec
	) {
		return StreamCodec.create(Filter::new,
				Filter::getIndex, StreamCodecs.ofVarInt(),
				filter -> filter.predicate, predicateStreamCodec,
				filter -> filter.input, STREAM_ID_STREAM_CODEC,
				filter -> filter.output, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(11)
	StreamCodec<ConsumerOfId> nodeConsumerOfId() {
		return StreamCodec.create(ConsumerOfId::new,
				ConsumerOfId::getIndex, StreamCodecs.ofVarInt(),
				consumerOfId -> consumerOfId.id, StreamCodecs.ofString(),
				consumerOfId -> consumerOfId.partitionIndex, StreamCodecs.ofVarInt(),
				consumerOfId -> consumerOfId.maxPartitions, StreamCodecs.ofVarInt(),
				consumerOfId -> consumerOfId.input, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(12)
	StreamCodec<SupplierOfId> nodeSupplierOfId() {
		return StreamCodec.create(SupplierOfId::new,
				SupplierOfId::getIndex, StreamCodecs.ofVarInt(),
				supplierOfId -> supplierOfId.id, StreamCodecs.ofString(),
				supplierOfId -> supplierOfId.partitionIndex, StreamCodecs.ofVarInt(),
				supplierOfId -> supplierOfId.maxPartitions, StreamCodecs.ofVarInt(),
				supplierOfId -> supplierOfId.output, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(13)
	StreamCodec<Union> nodeUnion() {
		return StreamCodec.create(Union::new,
				Union::getIndex, StreamCodecs.ofVarInt(),
				Union::getInputs, StreamCodecs.ofList(STREAM_ID_STREAM_CODEC),
				union -> union.output, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(14)
	StreamCodec<ReduceSimple> nodeReduceSimple(
			StreamCodec<Function> functionStreamCodec,
			StreamCodec<Comparator> comparatorStreamCodec,
			StreamCodec<Reducer> reducerStreamCodec
	) {
		return StreamCodec.create(ReduceSimple::new,
				ReduceSimple::getIndex, StreamCodecs.ofVarInt(),
				reduceSimple -> reduceSimple.keyFunction, functionStreamCodec,
				reduceSimple -> reduceSimple.keyComparator, comparatorStreamCodec,
				reduceSimple -> reduceSimple.reducer, reducerStreamCodec,
				ReduceSimple::getInputs, StreamCodecs.ofList(STREAM_ID_STREAM_CODEC),
				reduceSimple -> reduceSimple.output, STREAM_ID_STREAM_CODEC
		);
	}
}


