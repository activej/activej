package io.activej.cube.etcd;

import io.activej.aggregation.AggregationChunk;
import io.activej.aggregation.ot.AggregationDiff;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.tuple.Tuple2;
import io.activej.cube.Cube;
import io.activej.cube.linear.MeasuresValidator;
import io.activej.cube.ot.CubeDiff;
import io.activej.etcd.EtcdEventProcessor;
import io.activej.etcd.EtcdUtils;
import io.activej.etcd.TxnOps;
import io.activej.etcd.codec.*;
import io.activej.etl.LogDiff;
import io.activej.etl.LogPositionDiff;
import io.activej.etl.json.JsonCodecs;
import io.activej.multilog.LogPosition;
import io.activej.ot.uplink.AsyncOTUplink;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Response;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.DeleteOption;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static io.activej.aggregation.json.JsonCodecs.ofPrimaryKey;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.entriesToLinkedHashMap;
import static io.activej.common.Utils.union;
import static io.activej.cube.linear.CubeMySqlOTUplink.NO_MEASURE_VALIDATION;
import static io.activej.etcd.EtcdUtils.*;
import static io.activej.json.JsonUtils.fromJson;
import static io.activej.json.JsonUtils.toJson;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.*;

public final class CubeEtcdOTUplink extends AbstractReactive
	implements AsyncOTUplink<Long, LogDiff<CubeDiff>, CubeEtcdOTUplink.UplinkProtoCommit> {

	private static final ByteSequence POS = byteSequenceFrom("pos.");
	private static final ByteSequence CUBE = byteSequenceFrom("cube.");
	private static final EtcdPrefixCodec<String> AGGREGATION_ID_CODEC = EtcdPrefixCodecs.ofTerminatingString('.');

	private final Client client;
	private final ByteSequence root;

	private EtcdPrefixCodec<String> aggregationIdCodec = AGGREGATION_ID_CODEC;
	private Map<String, EtcdKVCodec<Long, AggregationChunk>> chunkCodecsFactory;
	private MeasuresValidator measuresValidator = NO_MEASURE_VALIDATION;
	private ByteSequence prefixPos = POS;
	private ByteSequence prefixCube = CUBE;

	private CubeEtcdOTUplink(Reactor reactor, Client client, ByteSequence root) {
		super(reactor);
		this.client = client;
		this.root = root;
	}

	public static CubeEtcdOTUplink.Builder builder(Reactor reactor, Client client, ByteSequence root) {
		return new CubeEtcdOTUplink(reactor, client, root).new Builder();
	}

	public final class Builder extends AbstractBuilder<CubeEtcdOTUplink.Builder, CubeEtcdOTUplink> {
		private Builder() {}

		public Builder withChunkCodecsFactory(Map<String, EtcdKVCodec<Long, AggregationChunk>> chunkCodecsFactory) {
			CubeEtcdOTUplink.this.chunkCodecsFactory = chunkCodecsFactory;
			return this;
		}

		public Builder withChunkCodecsFactoryJson(Cube cube) {
			return withChunkCodecsFactory(
				cube.getAggregations().entrySet().stream()
					.collect(entriesToLinkedHashMap(aggregation ->
						new AggregationChunkJsonEtcdKVCodec(ofPrimaryKey(aggregation.getStructure()))))
			);
		}

		public Builder withMeasuresValidator(MeasuresValidator measuresValidator) {
			CubeEtcdOTUplink.this.measuresValidator = measuresValidator;
			return this;
		}

		public Builder withPrefixPos(ByteSequence prefixPos) {
			CubeEtcdOTUplink.this.prefixPos = prefixPos;
			return this;
		}

		public Builder withPrefixCube(ByteSequence prefixCube) {
			CubeEtcdOTUplink.this.prefixCube = prefixCube;
			return this;
		}

		public Builder withAggregationIdCodec(EtcdPrefixCodec<String> aggregationIdCodec) {
			CubeEtcdOTUplink.this.aggregationIdCodec = aggregationIdCodec;
			return this;
		}

		@Override
		protected CubeEtcdOTUplink doBuild() {
			return CubeEtcdOTUplink.this;
		}
	}

	@Override
	public Promise<FetchData<Long, LogDiff<CubeDiff>>> checkout() {
		checkInReactorThread(this);
		return Promise.ofCompletionStage(
			doCheckout(0L)
				.thenApply(response -> new FetchData<>(response.revision, 1L, List.of(
					LogDiff.of(
						response.positions.entrySet().stream().collect(entriesToLinkedHashMap(logPosition -> new LogPositionDiff(null, logPosition))),
						CubeDiff.of(response.chunks.entrySet().stream().collect(entriesToLinkedHashMap(AggregationDiff::of)))
					)
				))));
	}

	record CubeCheckoutResponse(long revision, Map<String, LogPosition> positions, Map<String, Set<AggregationChunk>> chunks) {}

	@SuppressWarnings("unchecked")
	private CompletableFuture<CubeCheckoutResponse> doCheckout(long revision) {
		return EtcdUtils.checkout(client, revision, new CheckoutRequest[]{
				CheckoutRequest.<String, LogPosition, LinkedHashMap<String, LogPosition>>ofMapEntry(
					root.concat(prefixPos),
					EtcdKVCodecs.ofMapEntry(EtcdKeyCodecs.ofString(), logPositionEtcdCodec()),
					entriesToLinkedHashMap()),
				CheckoutRequest.<Tuple2<String, AggregationChunk>, Map<String, Set<AggregationChunk>>>of(
					root.concat(prefixCube),
					EtcdKVCodecs.ofPrefixedEntry(aggregationIdCodec, chunkCodecsFactory::get),
					groupingBy(Tuple2::value1, mapping(Tuple2::value2, toSet())))
			},
			(header, objects) -> {
				var logPositions = (Map<String, LogPosition>) objects[0];
				var aggregationChunks = (Map<String, Set<AggregationChunk>>) objects[1];

				for (var entry : aggregationChunks.entrySet()) {
					for (AggregationChunk chunk : entry.getValue()) {
						measuresValidator.validate(entry.getKey(), chunk.getMeasures());
					}
				}

				return new CubeCheckoutResponse(header.getRevision(), logPositions, aggregationChunks);
			}
		);
	}

	@Override
	public Promise<FetchData<Long, LogDiff<CubeDiff>>> fetch(Long currentCommitId) {
		checkInReactorThread(this);
		return Promise.ofCompletionStage(client.getKVClient().get(root))
			.then(response -> {
				long targetRevision = response.getKvs().isEmpty() ? response.getHeader().getRevision() : response.getKvs().get(0).getModRevision();
				return doFetch(currentCommitId, targetRevision)
					.map(logDiffs -> new FetchData<>(targetRevision, targetRevision, logDiffs));
			});
	}

	private Promise<List<LogDiff<CubeDiff>>> doFetch(long revisionFrom, long revisionTo) {
		checkInReactorThread(this);
		checkArgument(revisionFrom <= revisionTo);
		if (revisionTo == revisionFrom) return Promise.of(emptyList());
		SettablePromise<List<LogDiff<CubeDiff>>> promise = new SettablePromise<>();
		final AtomicReference<Watch.Watcher> etcdWatcherRef = new AtomicReference<>();
		reactor.startExternalTask();
		etcdWatcherRef.set(EtcdUtils.watch(client, revisionFrom + 1, new WatchRequest[]{
				WatchRequest.<String, LogPosition, Map<String, LogPositionDiff>>ofMapEntry(
					root.concat(prefixPos),
					EtcdKVCodecs.ofMapEntry(EtcdKeyCodecs.ofString(), logPositionEtcdCodec()),
					new EtcdEventProcessor<String, Map.Entry<String, LogPosition>, Map<String, LogPositionDiff>>() {
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
				),
				WatchRequest.<Tuple2<String, Long>, Tuple2<String, AggregationChunk>, Map<String, AggregationDiff>>of(
					root.concat(prefixCube),
					EtcdKVCodecs.ofPrefixedEntry(aggregationIdCodec, chunkCodecsFactory::get),
					new EtcdEventProcessor<Tuple2<String, Long>, Tuple2<String, AggregationChunk>, Map<String, AggregationDiff>>() {
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
				),
			},
			new io.activej.etcd.Listener<Object[]>() {
				LogDiff<CubeDiff> logDiff = LogDiff.empty();

				@SuppressWarnings("unchecked")
				@Override
				public void onNext(Response.Header header, Object[] operation) throws MalformedDataException {
					checkArgument(header.getRevision() <= revisionTo);

					var logPositionDiffs = (Map<String, LogPositionDiff>) operation[0];
					var aggregationDiffs = (Map<String, AggregationDiff>) operation[1];

					for (var entry : aggregationDiffs.entrySet()) {
						for (AggregationChunk addedChunk : entry.getValue().getAddedChunks()) {
							measuresValidator.validate(entry.getKey(), addedChunk.getMeasures());
						}
					}

					this.logDiff = LogDiff.reduce(List.of(this.logDiff, LogDiff.of(logPositionDiffs, CubeDiff.of(aggregationDiffs))), CubeDiff::reduce);

					if (header.getRevision() == revisionTo) {
						reactor.execute(() -> promise.trySet(List.of(logDiff)));
						etcdWatcherRef.get().close();
					}
				}

				@Override
				public void onError(Throwable throwable) {
					reactor.submit(() -> promise.trySetException((Exception) throwable));
				}

				@Override
				public void onCompleted() {
					reactor.completeExternalTask();
				}
			}));
		return promise;
	}

	@Override
	public Promise<UplinkProtoCommit> createProtoCommit(Long parent, List<LogDiff<CubeDiff>> diffs, long parentLevel) {
		checkInReactorThread(this);
		return Promise.of(new UplinkProtoCommit(parent, diffs));
	}

	@Override
	public Promise<FetchData<Long, LogDiff<CubeDiff>>> push(UplinkProtoCommit protoCommit) {
		checkInReactorThread(this);
		return Promise.ofCompletionStage(
				EtcdUtils.executeTxnOps(client, root, txnOps -> {
						touch(txnOps, ByteSequence.EMPTY);
						for (LogDiff<CubeDiff> diff : protoCommit.diffs) {
							saveCubeLogDiff(txnOps, diff);
						}
					})
					.thenCompose(txnResponse ->
						txnResponse.isSucceeded() ?
							completedFuture(txnResponse) :
							failedFuture(new IOException("Transaction failed")))
			)
			.then(txnResponse ->
				doFetch(protoCommit.parentRevision(), txnResponse.getHeader().getRevision() - 1)
					.map(logDiffs -> new FetchData<>(txnResponse.getHeader().getRevision(), txnResponse.getHeader().getRevision(), logDiffs)));
	}

	public void saveCubeLogDiff(TxnOps txn, LogDiff<CubeDiff> logDiff) {
		savePositions(txn.child(prefixPos), logDiff.getPositions());
		for (CubeDiff diff : logDiff.getDiffs()) {
			saveCubeDiff(txn.child(prefixCube), diff);
		}
	}

	public void savePositions(TxnOps txn, Map<String, LogPositionDiff> positions) {
		checkAndInsert(txn,
			EtcdKVCodecs.ofMapEntry(EtcdKeyCodecs.ofString(), logPositionEtcdCodec()),
			positions.entrySet().stream().filter(diff -> diff.getValue().from().isInitial()).collect(entriesToLinkedHashMap(LogPositionDiff::to)));
		checkAndUpdate(txn,
			EtcdKVCodecs.ofMapEntry(EtcdKeyCodecs.ofString(), logPositionEtcdCodec()),
			positions.entrySet().stream().filter(diff -> !diff.getValue().from().isInitial()).collect(entriesToLinkedHashMap(LogPositionDiff::from)),
			positions.entrySet().stream().filter(diff -> !diff.getValue().from().isInitial()).collect(entriesToLinkedHashMap(LogPositionDiff::to)));
	}

	public void saveCubeDiff(TxnOps txn, CubeDiff cubeDiff) {
		for (var entry : cubeDiff.getDiffs().entrySet().stream().collect(entriesToLinkedHashMap(AggregationDiff::getRemovedChunks)).entrySet()) {
			String aggregationId = entry.getKey();
			checkAndDelete(
				txn.child(aggregationIdCodec.encodePrefix(new Prefix<>(aggregationId, ByteSequence.EMPTY))),
				chunkCodecsFactory.get(aggregationId),
				entry.getValue().stream().map(chunk -> (long) chunk.getChunkId()).toList());
		}
		for (var entry : cubeDiff.getDiffs().entrySet().stream().collect(entriesToLinkedHashMap(AggregationDiff::getAddedChunks)).entrySet()) {
			String aggregationId = entry.getKey();
			checkAndInsert(
				txn.child(aggregationIdCodec.encodePrefix(new Prefix<>(aggregationId, ByteSequence.EMPTY))),
				chunkCodecsFactory.get(aggregationId),
				entry.getValue());
		}
	}

	public void saveAggregationDiff(TxnOps txn, EtcdKVCodec<Long, AggregationChunk> codec, AggregationDiff aggregationDiff) {
		checkAndDelete(txn, codec, aggregationDiff.getRemovedChunks());
		checkAndInsert(txn, codec, aggregationDiff.getAddedChunks());
	}

	public record UplinkProtoCommit(long parentRevision, List<LogDiff<CubeDiff>> diffs) {}

	public static EtcdValueCodec<LogPosition> logPositionEtcdCodec() {
		return new EtcdValueCodec<>() {
			@Override
			public ByteSequence encodeValue(LogPosition value) {
				return byteSequenceFrom(toJson(JsonCodecs.ofLogPosition(), value));
			}

			@Override
			public LogPosition decodeValue(ByteSequence byteSequence) throws MalformedDataException {
				return fromJson(JsonCodecs.ofLogPosition(), byteSequence.toString());
			}
		};
	}

	public void delete() throws ExecutionException, InterruptedException {
		client.getKVClient()
			.delete(root,
				DeleteOption.builder()
					.isPrefix(true)
					.build())
			.get();
		client.getKVClient()
			.put(root, ByteSequence.EMPTY)
			.get();
	}

}
