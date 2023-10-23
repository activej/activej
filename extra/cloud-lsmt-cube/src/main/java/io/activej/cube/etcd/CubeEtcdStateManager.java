package io.activej.cube.etcd;

import io.activej.common.ApplicationSettings;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.common.tuple.Tuple2;
import io.activej.cube.CubeState;
import io.activej.cube.CubeStructure;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.ot.AggregationDiff;
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
import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.ExceptionStats;
import io.activej.multilog.LogPosition;
import io.activej.ot.StateManager;
import io.activej.promise.Promise;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Response;
import io.etcd.jetcd.options.DeleteOption;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Utils.entriesToLinkedHashMap;
import static io.activej.common.Utils.union;
import static io.activej.cube.aggregation.json.JsonCodecs.ofPrimaryKey;
import static io.activej.cube.etcd.CubeEtcdOTUplink.logPositionEtcdCodec;
import static io.activej.cube.etcd.EtcdUtils.*;
import static io.activej.etcd.EtcdUtils.*;
import static java.util.stream.Collectors.*;

public final class CubeEtcdStateManager extends AbstractEtcdStateManager<LogState<CubeDiff, CubeState>, LogDiff<CubeDiff>>
	implements StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>>, ConcurrentJmxBean {

	private static final Logger logger = LoggerFactory.getLogger(CubeEtcdStateManager.class);

	private static final Duration WATCH_RETRY_INTERVAL = ApplicationSettings.getDuration(CubeEtcdStateManager.class, "watchRetryInterval", Duration.ofSeconds(1));

	private final CubeStructure cubeStructure;

	private EtcdPrefixCodec<String> aggregationIdCodec = AGGREGATION_ID_CODEC;
	private Function<String, EtcdKVCodec<Long, AggregationChunk>> chunkCodecsFactory;
	private ByteSequence prefixPos = POS;
	private ByteSequence prefixChunk = CHUNK;
	private ByteSequence timestampKey = TIMESTAMP;

	private ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
	private CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	// region JMX
	private final ExceptionStats watchEtcdExceptionStats = ExceptionStats.create();
	private final ExceptionStats malformedDataExceptionStats = ExceptionStats.create();
	private Instant watchConnectionLastEstablishedAt = null;
	private Instant watchLastCompletedAt = null;
	// endregion

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

		public Builder withTimestampKey(ByteSequence timestampKey) {
			checkNotBuilt(this);
			CubeEtcdStateManager.this.timestampKey = timestampKey;
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

		public Builder withScheduledExecutor(ScheduledExecutorService scheduledExecutor) {
			checkNotBuilt(this);
			CubeEtcdStateManager.this.scheduledExecutor = scheduledExecutor;
			return this;
		}

		@Override
		protected CubeEtcdStateManager doBuild() {
			if (chunkCodecsFactory == null) {
				Map<String, AggregationChunkJsonEtcdKVCodec> collect = cubeStructure.getAggregationStructures().entrySet().stream()
					.collect(entriesToLinkedHashMap(structure ->
						new AggregationChunkJsonEtcdKVCodec(ofPrimaryKey(structure))));

				chunkCodecsFactory = collect::get;
			}

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
					cubeStructure.validateMeasures(entry.getKey(), chunk.getMeasures());
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

	@Override
	public Promise<Void> catchUp() {
		return push(List.of())
			.whenComplete(toLogger(logger, "catchUp", this));
	}

	@Override
	public Promise<Void> push(List<LogDiff<CubeDiff>> diffs) {
		LogDiff<CubeDiff> diff = LogDiff.reduce(diffs, CubeDiff::reduce);
		return Promise.ofCompletionStage(push(diff)).toVoid()
			.whenComplete(toLogger(logger, "push", diffs, this));
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void applyStateTransitions(LogState<CubeDiff, CubeState> state, Object[] operation) throws MalformedEtcdDataException {
		var logPositionDiffs = (Map<String, LogPositionDiff>) operation[0];
		var aggregationDiffs = (Map<String, AggregationDiff>) operation[1];

		for (var entry : aggregationDiffs.entrySet()) {
			for (AggregationChunk addedChunk : entry.getValue().getAddedChunks()) {
				try {
					cubeStructure.validateMeasures(entry.getKey(), addedChunk.getMeasures());
				} catch (MalformedDataException e) {
					throw new MalformedEtcdDataException(e.getMessage());
				}
			}
		}

		state.apply(LogDiff.of(logPositionDiffs, CubeDiff.of(aggregationDiffs)));
	}

	@Override
	protected void doPush(TxnOps txn, LogDiff<CubeDiff> transaction) {
		if (isCatchUp(transaction)) return;

		touchTimestamp(txn, timestampKey, now);
		saveCubeLogDiff(prefixPos, prefixChunk, aggregationIdCodec, chunkCodecsFactory, txn, transaction);
	}

	@Override
	protected void onWatchConnectionEstablished() {
		logger.trace("Watch connection to etcd server established");
		watchConnectionLastEstablishedAt = now.currentInstant();
	}

	@Override
	protected void onWatchError(Throwable throwable) {
		logger.warn("Error while watching keys", throwable);
		watchEtcdExceptionStats.recordException(throwable, this);
		if (throwable instanceof MalformedEtcdDataException) {
			malformedDataExceptionStats.recordException(throwable, this);
			watcher.close();
		}
	}

	@Override
	protected void onWatchCompleted() {
		logger.warn("Watch has been completed");
		watchLastCompletedAt = now.currentInstant();
		scheduledExecutor.schedule(this::watch, WATCH_RETRY_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
	}

	private static boolean isCatchUp(LogDiff<CubeDiff> diff) {
		return
			diff.getPositions().values().stream().allMatch(LogPositionDiff::isEmpty) &&
			diff.getDiffs().stream().allMatch(CubeDiff::isEmpty);
	}

	@VisibleForTesting
	public void delete() throws ExecutionException, InterruptedException {
		KV kvClient = client.getKVClient();
		kvClient
			.delete(root,
				DeleteOption.builder()
					.isPrefix(true)
					.build())
			.get();
		kvClient
			.put(root.concat(timestampKey), TOUCH_TIMESTAMP_CODEC.encodeValue(now.currentTimeMillis()))
			.get();
	}

	@Override
	public String toString() {
		return "CubeEtcdStateManager{" +
			   "root=" + root +
			   ", revision=" + getRevision() +
			   '}';
	}

	// region JMX getters
	@JmxAttribute
	public ExceptionStats getWatchEtcdExceptionStats() {
		return watchEtcdExceptionStats;
	}

	@JmxAttribute
	public ExceptionStats getMalformedDataExceptionStats() {
		return malformedDataExceptionStats;
	}

	@JmxAttribute
	public Instant getWatchLastCompletedAtAt() {
		return watchLastCompletedAt;
	}

	@JmxAttribute
	public Instant getWatchConnectionLastEstablishedAt() {
		return watchConnectionLastEstablishedAt;
	}

	@Override
	@JmxAttribute
	public long getRevision() {
		return super.getRevision();
	}

	@JmxAttribute
	public String getEtcdRoot() {
		return root.toString();
	}
	// endregion
}
