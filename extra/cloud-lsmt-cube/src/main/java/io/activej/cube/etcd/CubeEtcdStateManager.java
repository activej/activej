package io.activej.cube.etcd;

import io.activej.async.exception.AsyncCloseException;
import io.activej.async.process.AbstractAsyncCloseable;
import io.activej.common.ApplicationSettings;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.common.tuple.Tuple2;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.cube.CubeState;
import io.activej.cube.CubeStructure;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.ot.CubeDiff;
import io.activej.datastream.supplier.BlockingPutQueue;
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
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.collection.CollectionUtils.union;
import static io.activej.common.collection.CollectorUtils.entriesToLinkedHashMap;
import static io.activej.cube.aggregation.json.JsonCodecs.ofPrimaryKey;
import static io.activej.cube.etcd.CubeEtcdOTUplink.logPositionEtcdCodec;
import static io.activej.cube.etcd.EtcdUtils.*;
import static io.activej.etcd.EtcdUtils.*;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.util.stream.Collectors.*;

public final class CubeEtcdStateManager extends AbstractEtcdStateManager<LogState<CubeDiff, CubeState>, LogDiff<CubeDiff>>
	implements StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>>, ConcurrentJmxBean {

	private static final Logger logger = LoggerFactory.getLogger(CubeEtcdStateManager.class);

	public static final Duration WATCH_RETRY_INTERVAL = ApplicationSettings.getDuration(CubeEtcdStateManager.class, "watchRetryInterval", Duration.ofSeconds(1));
	public static final int STATE_SUBSCRIBER_BUFFER_SIZE = ApplicationSettings.getInt(CubeEtcdStateManager.class, "stateSubscriberBufferSize", 128);

	private final CubeStructure cubeStructure;

	private final Set<StateTransitionListener> listeners = ConcurrentHashMap.newKeySet();

	private EtcdPrefixCodec<String> aggregationIdCodec = AGGREGATION_ID_CODEC;
	private Function<String, EtcdKVCodec<Long, AggregationChunk>> chunkCodecsFactory;
	private ByteSequence prefixPos = POS;
	private ByteSequence prefixChunk = CHUNK;
	private ByteSequence timestampKey = TIMESTAMP;

	private ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
	private CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private volatile boolean nextWatchScheduled;
	private volatile boolean stopping;

	// region JMX
	private final ExceptionStats watchEtcdExceptionStats = ExceptionStats.create();
	private final ExceptionStats malformedDataExceptionStats = ExceptionStats.create();
	private Instant watchConnectionLastEstablishedAt = null;
	private Instant watchStateLastUpdatedAt = null;
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

	@Override
	public StateChangesSupplier<LogDiff<CubeDiff>> subscribeToStateChanges(Predicate<LogDiff<CubeDiff>> predicate) {
		return new StateChangesListener(predicate);
	}

	public void subscribeToStateTransitions(StateTransitionListener listener) {
		if (stopping) {
			listener.onStop();
			return;
		}
		listeners.add(listener);
	}

	public void unsubscribeFromStateTransitions(StateTransitionListener listener) {
		listeners.remove(listener);
	}

	@Override
	public void stop() {
		stopping = true;
		super.stop();
		for (StateTransitionListener listener : listeners) {
			listener.onStop();
		}
		listeners.clear();
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

		LogDiff<CubeDiff> diff = LogDiff.of(logPositionDiffs, CubeDiff.of(aggregationDiffs));
		state.apply(diff);
		watchStateLastUpdatedAt = now.currentInstant();
		notifyListeners(diff);
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
		}
	}

	@Override
	protected void onWatchCompleted() {
		logger.warn("Watch has been completed");
		watchLastCompletedAt = now.currentInstant();
		if (stopping) return;
		scheduleNextWatch();
	}

	private void scheduleNextWatch() {
		if (nextWatchScheduled) return;
		nextWatchScheduled = true;
		scheduledExecutor.schedule(() -> {
				nextWatchScheduled = false;
				watch();
			},
			WATCH_RETRY_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
	}

	private void notifyListeners(LogDiff<CubeDiff> diff) {
		for (StateTransitionListener listener : listeners) {
			listener.onStateChange(diff);
		}
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

	public interface StateTransitionListener {
		void onStateChange(LogDiff<CubeDiff> diff);

		void onStop();
	}

	private final class StateChangesListener extends AbstractAsyncCloseable
		implements StateTransitionListener, StateChangesSupplier<LogDiff<CubeDiff>> {

		private final Predicate<LogDiff<CubeDiff>> predicate;

		private final ChannelZeroBuffer<LogDiff<CubeDiff>> zeroBuffer = new ChannelZeroBuffer<>();
		private final Queue queue = new Queue();

		public StateChangesListener(Predicate<LogDiff<CubeDiff>> predicate) {
			this.predicate = predicate;
			subscribeToStateTransitions(this);
		}

		@Override
		public void onStateChange(LogDiff<CubeDiff> diff) {
			if (!predicate.test(diff)) return;

			if (queue.isSaturated()) {
				unsubscribeFromStateTransitions(this);
				logger.error("Queue is saturated");
				Exception exception = new AsyncCloseException("State changes rate exceed supplier rate");
				reactor.execute(() -> closeEx(exception));
				return;
			}

			try {
				queue.put(diff);
			} catch (InterruptedException ignored) {
				throw new AssertionError("Should not happen");
			}
		}

		@Override
		public Promise<LogDiff<CubeDiff>> get() {
			checkInReactorThread(reactor);

			if (!queue.isEmpty()) {
				return Promise.of(queue.take());
			}

			return zeroBuffer.take();
		}

		@Override
		public void onStop() {
			reactor.execute(this::close);
		}

		@Override
		public void onClosed(Exception e) {
			unsubscribeFromStateTransitions(this);
			zeroBuffer.closeEx(e);
			queue.close();
		}

		private class Queue extends BlockingPutQueue<LogDiff<CubeDiff>> {
			public Queue() {
				super(STATE_SUBSCRIBER_BUFFER_SIZE);
			}

			@Override
			protected void onMoreData() {
				if (zeroBuffer.isSaturated()) return;

				zeroBuffer.put(take());
			}
		}
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
	public @Nullable Instant getWatchLastCompletedAt() {
		return watchLastCompletedAt;
	}

	@JmxAttribute
	public @Nullable Instant getWatchConnectionLastEstablishedAt() {
		return watchConnectionLastEstablishedAt;
	}

	@JmxAttribute
	public @Nullable Instant getWatchStateLastUpdatedAt() {
		return watchStateLastUpdatedAt;
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
