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
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.exception.CubeException;
import io.activej.cube.ot.CubeDiff;
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

import java.util.*;
import java.util.stream.Collectors;

import static io.activej.common.Utils.entriesToLinkedHashMap;
import static io.activej.reactor.Reactive.checkInReactorThread;

public final class CubeConsolidator extends AbstractReactive
	implements ReactiveJmxBeanWithStats {

	private static final Logger logger = LoggerFactory.getLogger(CubeConsolidator.class);

	private final StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager;
	private final CubeStructure structure;
	private final CubeExecutor executor;

	private CubeConsolidator(StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager, CubeStructure structure, CubeExecutor executor) {
		super(executor.getReactor());
		this.stateManager = stateManager;
		this.structure = structure;
		this.executor = executor;
	}

	public static CubeConsolidator create(StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager, CubeStructure structure, CubeExecutor executor) {
		return new CubeConsolidator(stateManager, structure, executor);
	}

	public Promise<CubeDiff> consolidate(ConsolidationStrategy strategy) {
		checkInReactorThread(this);
		logger.info("Launching consolidation");

		Map<String, AggregationDiff> map = new HashMap<>();
		List<AsyncRunnable> runnables = new ArrayList<>();

		Map<String, AggregationExecutor> aggregationExecutors = executor.getAggregationExecutors();

		for (String aggregationId : structure.getAggregationIds()) {
			AggregationExecutor aggregationExecutor = aggregationExecutors.get(aggregationId);

			runnables.add(() -> {
				int maxChunksToConsolidate = aggregationExecutor.getMaxChunksToConsolidate();
				int chunkSize = aggregationExecutor.getChunkSize();
				List<AggregationChunk> chunks = stateManager.query(state ->
					strategy.getChunksForConsolidation(
						aggregationId,
						state.getDataState().getAggregationState(aggregationId),
						maxChunksToConsolidate,
						chunkSize
					));
				return Promise.complete()
					.then(() -> chunks.isEmpty() ?
						Promise.of(AggregationDiff.empty()) :
						aggregationExecutor.consolidate(chunks))
					.whenResult(diff -> {if (!diff.isEmpty()) map.put(aggregationId, diff);})
					.mapException(e -> new CubeException("Failed to consolidate aggregation '" + aggregationId + '\'', e))
					.toVoid();
			});
		}

		return Promises.sequence(runnables).map($ -> CubeDiff.of(map));
	}

	public CubeStructure getStructure() {
		return structure;
	}

	public StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> getStateManager() {
		return stateManager;
	}

	public CubeExecutor getExecutor() {
		return executor;
	}

	public interface ConsolidationStrategy {
		List<AggregationChunk> getChunksForConsolidation(String id, AggregationState state, int maxChunksToConsolidate, int chunkSize);

		static ConsolidationStrategy minKey() {
			return minKey(Set.of());
		}

		static ConsolidationStrategy minKey(Set<Object> lockedChunkIds) {
			return (id, state, maxChunksToConsolidate, chunkSize) ->
				state.findChunksForConsolidationMinKey(
					maxChunksToConsolidate,
					chunkSize,
					lockedChunkIds
				);
		}

		static ConsolidationStrategy hotSegment() {
			return hotSegment(Set.of());
		}

		static ConsolidationStrategy hotSegment(Set<Object> lockedChunkIds) {
			return (id, state, maxChunksToConsolidate, chunkSize) ->
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
