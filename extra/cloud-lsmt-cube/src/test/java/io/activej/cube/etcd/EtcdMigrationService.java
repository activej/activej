package io.activej.cube.etcd;

import io.activej.common.builder.AbstractBuilder;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.cube.AggregationState;
import io.activej.cube.CubeState;
import io.activej.cube.CubeStructure;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.ot.CubeDiff;
import io.activej.etcd.codec.kv.EtcdKVCodec;
import io.activej.etcd.codec.prefix.EtcdPrefixCodec;
import io.activej.etcd.exception.EtcdException;
import io.activej.etl.LogDiff;
import io.activej.etl.LogPositionDiff;
import io.activej.etl.LogState;
import io.activej.multilog.LogPosition;
import io.activej.ot.StateManager;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.options.GetOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.collection.CollectorUtils.entriesToLinkedHashMap;
import static io.activej.cube.aggregation.json.JsonCodecs.ofPrimaryKey;
import static io.activej.cube.etcd.EtcdUtils.*;
import static io.activej.etcd.EtcdUtils.executeTxnOps;
import static io.activej.etcd.EtcdUtils.touchTimestamp;
import static java.util.concurrent.CompletableFuture.failedFuture;

public final class EtcdMigrationService {
	private static final Logger logger = LoggerFactory.getLogger(EtcdMigrationService.class);

	private final StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager;
	private final KV client;
	private final ByteSequence root;

	private EtcdPrefixCodec<String> aggregationIdCodec = AGGREGATION_ID_CODEC;
	private Function<String, EtcdKVCodec<Long, AggregationChunk>> chunkCodecsFactory;

	private ByteSequence prefixPos = POS;
	private ByteSequence prefixChunk = CHUNK;
	private ByteSequence timestampKey = TIMESTAMP;

	private CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private EtcdMigrationService(
		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager,
		KV client,
		ByteSequence root
	) {
		this.stateManager = stateManager;
		this.client = client;
		this.root = root;
	}

	public static Builder builder(
		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager,
		KV client,
		ByteSequence root
	) {
		return new EtcdMigrationService(stateManager, client, root).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, EtcdMigrationService> {
		private Builder() {
		}

		public Builder withChunkCodecsFactory(Function<String, EtcdKVCodec<Long, AggregationChunk>> chunkCodecsFactory) {
			checkNotBuilt(this);
			EtcdMigrationService.this.chunkCodecsFactory = chunkCodecsFactory;
			return this;
		}

		public Builder withChunkCodecsFactoryJson(CubeStructure cubeStructure) {
			checkNotBuilt(this);
			Map<String, AggregationChunkJsonEtcdKVCodec> collect = cubeStructure.getAggregationStructures().entrySet().stream()
				.collect(entriesToLinkedHashMap(structure ->
					new AggregationChunkJsonEtcdKVCodec(ofPrimaryKey(structure))));
			return withChunkCodecsFactory(collect::get);
		}

		public Builder withPrefixPos(ByteSequence prefixPos) {
			checkNotBuilt(this);
			EtcdMigrationService.this.prefixPos = prefixPos;
			return this;
		}

		public Builder withPrefixChunk(ByteSequence prefixChunk) {
			checkNotBuilt(this);
			EtcdMigrationService.this.prefixChunk = prefixChunk;
			return this;
		}

		public Builder withTimestampKey(ByteSequence timestampKey) {
			checkNotBuilt(this);
			EtcdMigrationService.this.timestampKey = timestampKey;
			return this;
		}

		public Builder withAggregationIdCodec(EtcdPrefixCodec<String> aggregationIdCodec) {
			checkNotBuilt(this);
			EtcdMigrationService.this.aggregationIdCodec = aggregationIdCodec;
			return this;
		}

		public Builder withCurrentTimeProvider(CurrentTimeProvider now) {
			checkNotBuilt(this);
			EtcdMigrationService.this.now = now;
			return this;
		}

		@Override
		protected EtcdMigrationService doBuild() {
			checkNotNull(chunkCodecsFactory, "Chunk codecs factory is required");
			return EtcdMigrationService.this;
		}
	}

	public CompletableFuture<TxnResponse> migrate() {
		logger.trace("Starting migration of state to 'etcd'. Root '{}', chunk prefix {}, position prefix {}",
			root, prefixChunk, prefixPos);

		return client.get(root, GetOption.builder().isPrefix(true).build())
			.thenCompose(getResponse -> {
				if (!getResponse.getKvs().isEmpty()) {
					return failedFuture(new EtcdException("Root '" + root + "' namespace is not empty"));
				}

				return migrateState();
			})
			.whenComplete((txnResponse, e) -> {
				if (e == null) {
					logger.info("State successfully migrated to 'etcd'");
				} else {
					logger.warn("Failed to migrate state to 'etcd'", e);
				}
			});
	}

	private LogDiff<CubeDiff> stateToLogDiff(LogState<CubeDiff, CubeState> logState) {
		Map<String, LogPosition> positions = logState.getPositions();
		Map<String, LogPositionDiff> positionDiffs = new HashMap<>(positions.size());

		for (Entry<String, LogPosition> entry : positions.entrySet()) {
			positionDiffs.put(entry.getKey(), new LogPositionDiff(LogPosition.initial(), entry.getValue()));
		}

		CubeState cubeState = logState.getDataState();
		Map<String, AggregationState> aggregationStates = cubeState.getAggregationStates();
		Map<String, AggregationDiff> aggregationDiffs = new HashMap<>(aggregationStates.size());

		for (Entry<String, AggregationState> entry : aggregationStates.entrySet()) {
			AggregationState aggregationState = entry.getValue();
			Map<Long, AggregationChunk> chunksMap = aggregationState.getChunks();
			Set<AggregationChunk> addedChunks = new HashSet<>(chunksMap.values());
			aggregationDiffs.put(entry.getKey(), AggregationDiff.of(addedChunks));
		}

		CubeDiff cubeDiff = CubeDiff.of(aggregationDiffs);

		return LogDiff.of(positionDiffs, List.of(cubeDiff));
	}

	private CompletableFuture<TxnResponse> migrateState() {
		LogDiff<CubeDiff> logDiff = stateManager.query(this::stateToLogDiff);

		if (logger.isTraceEnabled()) {
			logger.trace("Migrating state of {} log positions and {} aggregations with {} chunks",
				logDiff.getPositions().size(),
				logDiff.getDiffs().get(0).getDiffs().size(),
				logDiff.getDiffs().get(0).addedChunks().count());
		}

		return executeTxnOps(client, root, txnOps -> {
			txnOps.cmp(timestampKey, Cmp.Op.EQUAL, CmpTarget.createRevision(0));

			touchTimestamp(txnOps, timestampKey, now);
			saveCubeLogDiff(prefixPos, prefixChunk, aggregationIdCodec, chunkCodecsFactory, txnOps, logDiff);
		});
	}
}
