/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.cube;

import io.activej.async.function.AsyncRunnable;
import io.activej.common.builder.AbstractBuilder;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.ChunksAlreadyLockedException;
import io.activej.cube.aggregation.IChunkLocker;
import io.activej.cube.aggregation.NoOpChunkLocker;
import io.activej.cube.aggregation.ot.ProtoAggregationDiff;
import io.activej.cube.exception.CubeException;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.ProtoCubeDiff;
import io.activej.etl.LogDiff;
import io.activej.etl.LogState;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.ot.StateManager;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.activej.async.util.LogUtils.thisMethod;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.collection.CollectorUtils.entriesToLinkedHashMap;
import static io.activej.cube.aggregation.util.Utils.collectChunkIds;
import static io.activej.cube.aggregation.util.Utils.materializeProtoDiff;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.util.stream.Collectors.toSet;

public final class CubeConsolidator extends AbstractReactive
	implements ReactiveJmxBeanWithStats {

	private static final Logger logger = LoggerFactory.getLogger(CubeConsolidator.class);

	private final StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager;
	private final CubeExecutor executor;

	private final Map<String, IChunkLocker> lockers = new HashMap<>();
	private Function<String, IChunkLocker> chunkLockerFactory = $ -> NoOpChunkLocker.create(reactor);

	private CubeConsolidator(StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager, CubeExecutor executor) {
		super(executor.getReactor());
		this.stateManager = stateManager;
		this.executor = executor;
	}

	public static CubeConsolidator create(StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager, CubeExecutor executor) {
		return builder(stateManager, executor).build();
	}

	public static Builder builder(StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager, CubeExecutor executor) {
		return new CubeConsolidator(stateManager, executor).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, CubeConsolidator> {
		private Builder() {}

		public Builder withChunkLockerFactory(Function<String, IChunkLocker> factory) {
			checkNotBuilt(this);
			CubeConsolidator.this.chunkLockerFactory = checkNotNull(factory);
			return this;
		}

		@Override
		protected CubeConsolidator doBuild() {
			return CubeConsolidator.this;
		}
	}

	public StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> getStateManager() {
		return stateManager;
	}

	public Promise<CubeDiff> consolidate(List<String> aggregationIds, ConsolidationStrategy strategy) {
		checkInReactorThread(this);
		logger.info("Launching consolidation for aggregations {}", aggregationIds);

		if (aggregationIds.isEmpty()) return Promise.of(CubeDiff.empty());

		Map<String, List<AggregationChunk>> lockedChunks = new HashMap<>();
		Map<String, ProtoAggregationDiff> diffMap = new HashMap<>();

		return Promises.toList(aggregationIds.stream()
				.map(aggregationId -> findAndLockChunksForConsolidation(aggregationId, strategy)
					.<AsyncRunnable>map(chunks -> () -> {
						if (chunks.isEmpty()) return Promise.complete();
						lockedChunks.put(aggregationId, chunks);
						return consolidateAggregationChunks(aggregationId, chunks)
							.whenResult(diff -> {if (!diff.isEmpty()) diffMap.put(aggregationId, diff);})
							.toVoid();
					})))
			.then(asyncRunnables -> Promises.sequence(asyncRunnables))
			.then(() -> finishConsolidation(diffMap))
			.then((cubeDiff, e) -> releaseChunks(lockedChunks)
				.then(() -> Promise.of(cubeDiff, e)))
			.whenComplete(toLogger(logger, thisMethod(), aggregationIds));
	}

	private List<AggregationChunk> findChunksForConsolidation(String aggregationId, Set<Long> lockedChunkIds, ConsolidationStrategy strategy) {
		AggregationExecutor aggregationExecutor = executor.getAggregationExecutors().get(aggregationId);
		int maxChunksToConsolidate = aggregationExecutor.getMaxChunksToConsolidate();
		int chunkSize = aggregationExecutor.getChunkSize();
		return stateManager.query(state -> strategy.getChunksForConsolidation(
			aggregationId,
			state.getDataState().getAggregationState(aggregationId),
			maxChunksToConsolidate,
			chunkSize,
			lockedChunkIds
		));
	}

	private Promise<List<AggregationChunk>> findAndLockChunksForConsolidation(
		String aggregationId, ConsolidationStrategy strategy
	) {
		IChunkLocker locker = ensureLocker(aggregationId);

		return Promises.retry(($, e) -> !(e instanceof ChunksAlreadyLockedException),
			() -> locker.getLockedChunks()
				.map(lockedChunkIds -> findChunksForConsolidation(aggregationId, lockedChunkIds, strategy))
				.then(chunks -> {
					if (chunks.isEmpty()) {
						logger.info("Nothing to consolidate in aggregation '{}'", aggregationId);
						return Promise.of(chunks);
					}
					return locker.lockChunks(collectChunkIds(chunks))
						.map($ -> chunks);
				}));
	}

	private Promise<ProtoAggregationDiff> consolidateAggregationChunks(String aggregationId, List<AggregationChunk> chunks) {
		AggregationExecutor aggregationExecutor = executor.getAggregationExecutors().get(aggregationId);
		return aggregationExecutor.consolidate(chunks)
			.mapException(e -> new CubeException("Failed to consolidate aggregation '" + aggregationId + '\'', e))
			.whenComplete(toLogger(logger, thisMethod(), aggregationId));
	}

	private IChunkLocker ensureLocker(String aggregationId) {
		return lockers.computeIfAbsent(aggregationId, $ -> chunkLockerFactory.apply(aggregationId));
	}

	private static Set<String> addedProtoChunks(ProtoCubeDiff protoCubeDiff) {
		return protoCubeDiff.addedProtoChunks().collect(toSet());
	}

	private Promise<CubeDiff> finishConsolidation(Map<String, ProtoAggregationDiff> map) {
		if (map.isEmpty()) return Promise.of(CubeDiff.empty());
		ProtoCubeDiff protoCubeDiff = new ProtoCubeDiff(map);
		return executor.getAggregationChunkStorage().finish(addedProtoChunks(protoCubeDiff))
			.mapException(e -> new CubeException("Failed to finalize chunks in storage", e))
			.then(chunkIds -> {
				CubeDiff cubeDiff = materializeProtoDiff(protoCubeDiff, chunkIds);
				return stateManager.push(List.of(LogDiff.forCurrentPosition(cubeDiff)))
					.mapException(e -> new CubeException("Failed to synchronize state after consolidation, resetting", e))
					.map($ -> cubeDiff);
			});
	}

	private Promise<Void> releaseChunks(Map<String, List<AggregationChunk>> chunks) {
		if (chunks.isEmpty()) return Promise.complete();
		return Promises.all(chunks.entrySet().stream()
			.map(entry -> {
				String aggregationId = entry.getKey();
				Set<Long> chunkIds = collectChunkIds(entry.getValue());
				return ensureLocker(aggregationId).releaseChunks(chunkIds)
					.map(($, e) -> {
						if (e != null) {
							logger.warn("Failed to release chunks: {} in aggregation {}",
								chunkIds, aggregationId, e);
						}
						return null;
					});
			}));
	}

	public interface ConsolidationStrategy {
		List<AggregationChunk> getChunksForConsolidation(String id, AggregationState state, int maxChunksToConsolidate, int chunkSize, Set<Long> lockedChunkIds);

		static ConsolidationStrategy minKey() {
			return (id, state, maxChunksToConsolidate, chunkSize, lockedChunkIds) ->
				state.findChunksForConsolidationMinKey(
					maxChunksToConsolidate,
					chunkSize,
					lockedChunkIds
				);
		}

		static ConsolidationStrategy hotSegment() {
			return (id, state, maxChunksToConsolidate, chunkSize, lockedChunkIds) ->
				state.findChunksForConsolidationHotSegment(
					maxChunksToConsolidate,
					lockedChunkIds
				);
		}
	}

	@JmxOperation
	public Map<String, String> getIrrelevantChunksIds() {
		return stateManager.query(state ->
			state.getDataState().getIrrelevantChunks().entrySet().stream()
				.collect(entriesToLinkedHashMap(chunks -> chunks.stream()
					.map(chunk -> String.valueOf(chunk.getChunkId()))
					.collect(Collectors.joining(", "))))
		);
	}
}
