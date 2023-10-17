package io.activej.cube.etcd;

import io.activej.common.builder.AbstractBuilder;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.common.tuple.Tuple2;
import io.activej.cube.CubeState;
import io.activej.cube.CubeStructure;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.linear.MeasuresValidator;
import io.activej.cube.ot.CubeDiff;
import io.activej.etcd.EtcdEventProcessor;
import io.activej.etcd.EtcdUtils;
import io.activej.etcd.TxnOps;
import io.activej.etcd.codec.key.EtcdKeyCodecs;
import io.activej.etcd.codec.kv.EtcdKVCodec;
import io.activej.etcd.codec.kv.EtcdKVCodecs;
import io.activej.etcd.codec.prefix.EtcdPrefixCodec;
import io.activej.etcd.exception.MalformedEtcdDataException;
import io.activej.etcd.state.AbstractEtcdStateManager;
import io.activej.etl.LogDiff;
import io.activej.etl.LogPositionDiff;
import io.activej.etl.LogState;
import io.activej.multilog.LogPosition;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Response;
import io.etcd.jetcd.options.DeleteOption;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.Utils.entriesToLinkedHashMap;
import static io.activej.common.Utils.union;
import static io.activej.cube.aggregation.json.JsonCodecs.ofPrimaryKey;
import static io.activej.cube.etcd.CubeEtcdOTUplink.logPositionEtcdCodec;
import static io.activej.cube.etcd.EtcdUtils.*;
import static io.activej.cube.linear.CubeMySqlOTUplink.NO_MEASURE_VALIDATION;
import static io.activej.etcd.EtcdUtils.*;
import static java.util.stream.Collectors.*;

public final class CubeEtcdStateManager extends AbstractEtcdStateManager<LogState<CubeDiff, CubeState>, List<LogDiff<CubeDiff>>> {
	private final CubeStructure cubeStructure;

	private EtcdPrefixCodec<String> aggregationIdCodec = AGGREGATION_ID_CODEC;
	private Function<String, EtcdKVCodec<Long, AggregationChunk>> chunkCodecsFactory;
	private MeasuresValidator measuresValidator = NO_MEASURE_VALIDATION;
	private ByteSequence prefixPos = POS;
	private ByteSequence prefixChunk = CHUNK;

	private CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private CubeEtcdStateManager(
		Client client,
		ByteSequence root,
		CheckoutRequest<?, ?>[] checkoutRequests,
		WatchRequest<?, ?, ?>[] watchRequests,
		CubeStructure cubeStructure
	) {
		super(client, root, checkoutRequests, watchRequests);
		this.cubeStructure = cubeStructure;
	}

	public static Builder builder(Client client, ByteSequence root, CubeStructure cubeStructure) {
		CheckoutRequest<?, ?>[] checkoutRequests = new CheckoutRequest[2];
		WatchRequest<?, ?, ?>[] watchRequests = new WatchRequest[2];
		CubeEtcdStateManager cubeEtcdStateManager = new CubeEtcdStateManager(client, root, checkoutRequests, watchRequests, cubeStructure);
		return cubeEtcdStateManager.new Builder(root, checkoutRequests, watchRequests);
	}

	public final class Builder extends AbstractBuilder<Builder, CubeEtcdStateManager> {
		private final ByteSequence root;
		private final EtcdUtils.CheckoutRequest<?, ?>[] checkoutRequests;
		private final EtcdUtils.WatchRequest<?, ?, ?>[] watchRequests;

		private Builder(ByteSequence root, CheckoutRequest<?, ?>[] checkoutRequests, WatchRequest<?, ?, ?>[] watchRequests) {
			this.root = root;
			this.checkoutRequests = checkoutRequests;
			this.watchRequests = watchRequests;
		}

		public Builder withChunkCodecsFactory(Function<String, EtcdKVCodec<Long, AggregationChunk>> chunkCodecsFactory) {
			checkNotBuilt(this);
			CubeEtcdStateManager.this.chunkCodecsFactory = chunkCodecsFactory;
			return this;
		}

		public Builder withChunkCodecsFactoryJson(CubeStructure cubeStructure) {
			checkNotBuilt(this);
			Map<String, AggregationChunkJsonEtcdKVCodec> collect = cubeStructure.getAggregationStructures().entrySet().stream()
				.collect(entriesToLinkedHashMap(structure ->
					new AggregationChunkJsonEtcdKVCodec(ofPrimaryKey(structure))));
			return withChunkCodecsFactory(collect::get);
		}

		public Builder withMeasuresValidator(MeasuresValidator measuresValidator) {
			checkNotBuilt(this);
			CubeEtcdStateManager.this.measuresValidator = measuresValidator;
			return this;
		}

		public Builder withPrefixPos(ByteSequence prefixPos) {
			checkNotBuilt(this);
			CubeEtcdStateManager.this.prefixPos = prefixPos;
			return this;
		}

		public Builder withPrefixChunk(ByteSequence prefixChunk) {
			checkNotBuilt(this);
			CubeEtcdStateManager.this.prefixChunk = prefixChunk;
			return this;
		}

		public Builder withAggregationIdCodec(EtcdPrefixCodec<String> aggregationIdCodec) {
			checkNotBuilt(this);
			CubeEtcdStateManager.this.aggregationIdCodec = aggregationIdCodec;
			return this;
		}

		public Builder withCurrentTimeProvider(CurrentTimeProvider now) {
			checkNotBuilt(this);
			CubeEtcdStateManager.this.now = now;
			return this;
		}

		@Override
		protected CubeEtcdStateManager doBuild() {
			checkNotNull(chunkCodecsFactory, "Chunk codecs factory is required");

			checkoutRequests[0] = CheckoutRequest.ofMapEntry(
				root.concat(prefixPos),
				EtcdKVCodecs.ofMapEntry(EtcdKeyCodecs.ofString(), logPositionEtcdCodec()),
				entriesToLinkedHashMap());
			checkoutRequests[1] = CheckoutRequest.of(
				root.concat(prefixChunk),
				EtcdKVCodecs.ofPrefixedEntry(aggregationIdCodec, chunkCodecsFactory),
				groupingBy(Tuple2::value1, mapping(Tuple2::value2, toSet())));

			watchRequests[0] = WatchRequest.<String, LogPosition, Map<String, LogPositionDiff>>ofMapEntry(
				root.concat(prefixPos),
				EtcdKVCodecs.ofMapEntry(EtcdKeyCodecs.ofString(), logPositionEtcdCodec()),
				new EtcdEventProcessor<>() {
					@Override
					public Map<String, LogPositionDiff> createEventsAccumulator() {
						return new LinkedHashMap<>();
					}

					@Override
					public void onPut(Map<String, LogPositionDiff> accumulator, Map.Entry<String, LogPosition> entry) {
						accumulator.put(entry.getKey(), new LogPositionDiff(null, entry.getValue()));
					}

					@Override
					public void onDelete(Map<String, LogPositionDiff> accumulator, String key) {
						throw new UnsupportedOperationException();
					}
				}
			);
			watchRequests[1] = WatchRequest.<Tuple2<String, Long>, Tuple2<String, AggregationChunk>, Map<String, AggregationDiff>>of(
				root.concat(prefixChunk),
				EtcdKVCodecs.ofPrefixedEntry(aggregationIdCodec, chunkCodecsFactory),
				new EtcdEventProcessor<>() {
					@Override
					public Map<String, AggregationDiff> createEventsAccumulator() {
						return new LinkedHashMap<>();
					}

					@Override
					public void onPut(Map<String, AggregationDiff> accumulator, Tuple2<String, AggregationChunk> kv) {
						accumulator.compute(kv.value1(), (aggregationId, aggregationDiff) ->
							aggregationDiff == null ?
								AggregationDiff.of(Set.of(kv.value2()), Set.of()) :
								AggregationDiff.of(union(aggregationDiff.getAddedChunks(), Set.of(kv.value2())), aggregationDiff.getRemovedChunks()));
					}

					@Override
					public void onDelete(Map<String, AggregationDiff> accumulator, Tuple2<String, Long> key) {
						accumulator.compute(key.value1(), (aggregationId, aggregationDiff) ->
							aggregationDiff == null ?
								AggregationDiff.of(Set.of(), Set.of(AggregationChunk.ofId(key.value2()))) :
								AggregationDiff.of(aggregationDiff.getAddedChunks(), union(aggregationDiff.getRemovedChunks(), Set.of(AggregationChunk.ofId(key.value2())))));
					}
				}
			);

			return CubeEtcdStateManager.this;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected LogState<CubeDiff, CubeState> finishState(Response.Header header, Object[] checkoutObjects) throws MalformedEtcdDataException {
		LogState<CubeDiff, CubeState> state = LogState.create(CubeState.create(cubeStructure));
		state.init();
		var logPositions = (Map<String, LogPosition>) checkoutObjects[0];
		var aggregationChunks = (Map<String, Set<AggregationChunk>>) checkoutObjects[1];

		for (var entry : aggregationChunks.entrySet()) {
			for (AggregationChunk chunk : entry.getValue()) {
				try {
					measuresValidator.validate(entry.getKey(), chunk.getMeasures());
				} catch (MalformedDataException e) {
					throw new MalformedEtcdDataException(e.getMessage());
				}
			}
		}

		Map<String, LogPositionDiff> positions = logPositions.entrySet().stream()
			.collect(entriesToLinkedHashMap(logPosition -> new LogPositionDiff(null, logPosition)));

		CubeDiff cubeDiff = CubeDiff.of(aggregationChunks.entrySet().stream()
			.collect(entriesToLinkedHashMap(AggregationDiff::of)));

		state.apply(LogDiff.of(positions, cubeDiff));
		return state;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void applyStateTransitions(LogState<CubeDiff, CubeState> state, Object[] operation) throws MalformedEtcdDataException {
		var logPositionDiffs = (Map<String, LogPositionDiff>) operation[0];
		var aggregationDiffs = (Map<String, AggregationDiff>) operation[1];

		for (var entry : aggregationDiffs.entrySet()) {
			for (AggregationChunk addedChunk : entry.getValue().getAddedChunks()) {
				try {
					measuresValidator.validate(entry.getKey(), addedChunk.getMeasures());
				} catch (MalformedDataException e) {
					throw new MalformedEtcdDataException(e.getMessage());
				}
			}
		}

		state.apply(LogDiff.of(logPositionDiffs, CubeDiff.of(aggregationDiffs)));
	}

	@Override
	protected void doPush(TxnOps txn, List<LogDiff<CubeDiff>> transaction) {
		touchTimestamp(txn, ByteSequence.EMPTY, now);
		for (LogDiff<CubeDiff> diff : transaction) {
			saveCubeLogDiff(prefixPos, prefixChunk, aggregationIdCodec, chunkCodecsFactory, txn, diff);
		}
	}

	public void delete() throws ExecutionException, InterruptedException {
		client.getKVClient()
			.delete(root,
				DeleteOption.builder()
					.isPrefix(true)
					.build())
			.get();
		client.getKVClient()
			.put(root, TOUCH_TIMESTAMP_CODEC.encodeValue(now.currentTimeMillis()))
			.get();
	}
}
