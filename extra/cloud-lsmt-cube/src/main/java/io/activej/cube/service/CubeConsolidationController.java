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

package io.activej.cube.service;

import io.activej.async.function.AsyncRunnable;
import io.activej.common.builder.AbstractBuilder;
import io.activej.cube.CubeConsolidator;
import io.activej.cube.CubeConsolidator.ConsolidationStrategy;
import io.activej.cube.CubeStructure;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.exception.CubeException;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeDiffScheme;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.stats.ValueStats;
import io.activej.ot.StateManager;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.jmx.PromiseStats;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.activej.async.function.AsyncRunnables.reuse;
import static io.activej.async.util.LogUtils.thisMethod;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.Checks.checkState;
import static io.activej.common.collection.CollectorUtils.entriesToLinkedHashMap;
import static io.activej.reactor.Reactive.checkInReactorThread;

public final class CubeConsolidationController<D> extends AbstractReactive
	implements ReactiveJmxBeanWithStats {

	private static final Logger logger = LoggerFactory.getLogger(CubeConsolidationController.class);

	public static final Supplier<ConsolidationStrategy> DEFAULT_CONSOLIDATION_STRATEGY = new Supplier<>() {
		private boolean hotSegment = false;

		@Override
		public ConsolidationStrategy get() {
			hotSegment = !hotSegment;
			return hotSegment ?
				ConsolidationStrategy.hotSegment() :
				ConsolidationStrategy.minKey();
		}
	};

	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final CubeDiffScheme<D> cubeDiffScheme;
	private final CubeConsolidator cubeConsolidator;
	private final CubeStructure cubeStructure;
	private final StateManager<D, ?> stateManager;

	private final PromiseStats promiseConsolidate = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseConsolidateImpl = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseCleanupIrrelevantChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	private final ValueStats removedChunks = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final ValueStats removedChunksRecords = ValueStats.builder(DEFAULT_SMOOTHING_WINDOW)
		.withRate()
		.build();
	private final ValueStats addedChunks = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final ValueStats addedChunksRecords = ValueStats.builder(DEFAULT_SMOOTHING_WINDOW)
		.withRate()
		.build();
	private long lastNotEmptyConsolidationTimestamp;

	private Supplier<ConsolidationStrategy> strategySupplier = DEFAULT_CONSOLIDATION_STRATEGY;

	private boolean consolidating;
	private boolean cleaning;

	private CubeConsolidationController(
		Reactor reactor, CubeDiffScheme<D> cubeDiffScheme, CubeConsolidator cubeConsolidator, CubeStructure cubeStructure
	) {
		super(reactor);
		this.cubeDiffScheme = cubeDiffScheme;
		this.cubeConsolidator = cubeConsolidator;
		//noinspection unchecked
		this.stateManager = (StateManager<D, ?>) cubeConsolidator.getStateManager();
		this.cubeStructure = cubeStructure;
	}

	public static <D> CubeConsolidationController<D> create(
		Reactor reactor, CubeDiffScheme<D> cubeDiffScheme, CubeConsolidator cubeConsolidator, CubeStructure cubeStructure
	) {
		return builder(reactor, cubeDiffScheme, cubeConsolidator, cubeStructure).build();
	}

	public static <D> CubeConsolidationController<D>.Builder builder(
		Reactor reactor, CubeDiffScheme<D> cubeDiffScheme, CubeConsolidator cubeConsolidator, CubeStructure cubeStructure
	) {
		return new CubeConsolidationController<>(reactor, cubeDiffScheme, cubeConsolidator, cubeStructure).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, CubeConsolidationController<D>> {
		private Builder() {}

		public Builder withConsolidationStrategy(Supplier<ConsolidationStrategy> strategy) {
			checkNotBuilt(this);
			CubeConsolidationController.this.strategySupplier = checkNotNull(strategy);
			return this;
		}

		@Override
		protected CubeConsolidationController<D> doBuild() {
			return CubeConsolidationController.this;
		}
	}

	private final AsyncRunnable consolidate = reuse(this::doConsolidate);
	private final AsyncRunnable cleanupIrrelevantChunks = reuse(this::doCleanupIrrelevantChunks);

	@SuppressWarnings("UnusedReturnValue")
	public Promise<Void> consolidate() {
		checkInReactorThread(this);
		return consolidate.run();
	}

	@SuppressWarnings("UnusedReturnValue")
	public Promise<Void> cleanupIrrelevantChunks() {
		checkInReactorThread(this);
		return cleanupIrrelevantChunks.run();
	}

	private Promise<Void> doConsolidate() {
		checkInReactorThread(this);
		checkState(!cleaning, "Cannot consolidate and clean up irrelevant chunks at the same time");
		consolidating = true;
		logger.info("Launching consolidation");
		ConsolidationStrategy strategy = strategySupplier.get();
		return stateManager.catchUp()
			.mapException(e -> new CubeException("Failed to synchronize state prior to consolidation", e))
			.then(() -> doConsolidate(strategy))
			.whenComplete(this::logConsolidation)
			.whenComplete(promiseConsolidate.recordStats())
			.whenComplete(toLogger(logger, thisMethod(), stateManager))
			.whenComplete(() -> consolidating = false)
			.toVoid();
	}

	private Promise<List<String>> doConsolidate(ConsolidationStrategy strategy) {
		Set<String> aggregationIds = cubeStructure.getAggregationIds();
		List<String> consolidatedAggregations = new ArrayList<>(aggregationIds.size());
		List<String> aggregationsList = new ArrayList<>(aggregationIds);
		Collections.shuffle(aggregationsList);
		Stream<AsyncRunnable> runnables = aggregationsList.stream().map(aggregationId ->
			() -> cubeConsolidator.consolidate(List.of(aggregationId), strategy)
				.whenComplete(promiseConsolidateImpl.recordStats())
				.whenResult(this::cubeDiffJmx)
				.whenResult(diff -> {if (!diff.isEmpty()) consolidatedAggregations.add(aggregationId);})
				.toVoid()
		);
		return Promises.sequence(runnables).map($ -> consolidatedAggregations);
	}

	private Promise<Void> doCleanupIrrelevantChunks() {
		checkState(!consolidating, "Cannot consolidate and clean up irrelevant chunks at the same time");
		cleaning = true;
		return stateManager.catchUp()
			.mapException(e -> new CubeException("Failed to synchronize state prior to cleaning up irrelevant chunks", e))
			.then(() -> {
				Map<String, Set<AggregationChunk>> irrelevantChunks = cubeConsolidator.getStateManager().query(state -> state.getDataState().getIrrelevantChunks());
				if (irrelevantChunks.isEmpty()) {
					logger.info("Found no irrelevant chunks");
					return Promise.complete();
				}
				logger.info("Removing irrelevant chunks: {}", irrelevantChunks.keySet());
				Map<String, AggregationDiff> diffMap = irrelevantChunks.entrySet().stream()
					.collect(entriesToLinkedHashMap(chunksToRemove -> AggregationDiff.of(Set.of(), chunksToRemove)));
				CubeDiff cubeDiff = CubeDiff.of(diffMap);
				return stateManager.push(List.of(cubeDiffScheme.wrap(cubeDiff)))
					.mapException(e -> new CubeException("Failed to synchronize state after cleaning up irrelevant chunks, resetting", e))
					.whenResult(() -> cubeDiffJmx(cubeDiff));
			})
			.whenComplete(promiseCleanupIrrelevantChunks.recordStats())
			.whenComplete(toLogger(logger, thisMethod(), stateManager))
			.whenComplete(() -> cleaning = false);
	}

	private void cubeDiffJmx(CubeDiff cubeDiff) {
		long curAddedChunks = 0;
		long curAddedChunksRecords = 0;
		long curRemovedChunks = 0;
		long curRemovedChunksRecords = 0;

		for (String key : cubeDiff.keySet()) {
			AggregationDiff aggregationDiff = cubeDiff.get(key);
			curAddedChunks += aggregationDiff.getAddedChunks().size();
			for (AggregationChunk aggregationChunk : aggregationDiff.getAddedChunks()) {
				curAddedChunksRecords += aggregationChunk.getCount();
			}

			curRemovedChunks += aggregationDiff.getRemovedChunks().size();
			for (AggregationChunk aggregationChunk : aggregationDiff.getRemovedChunks()) {
				curRemovedChunksRecords += aggregationChunk.getCount();
			}
		}

		addedChunks.recordValue(curAddedChunks);
		addedChunksRecords.recordValue(curAddedChunksRecords);
		removedChunks.recordValue(curRemovedChunks);
		removedChunksRecords.recordValue(curRemovedChunksRecords);
	}

	private void logConsolidation(Collection<String> consolidated, Exception e) {
		if (e != null) logger.warn("Consolidation failed", e);
		else if (consolidated.isEmpty()) logger.info("Previous consolidation did not merge any chunks");
		else {
			logger.info("Consolidation finished. Launching consolidation task again.");
			lastNotEmptyConsolidationTimestamp = reactor.currentTimeMillis();
		}
	}

	@JmxAttribute
	public ValueStats getRemovedChunks() {
		return removedChunks;
	}

	@JmxAttribute
	public ValueStats getAddedChunks() {
		return addedChunks;
	}

	@JmxAttribute
	public ValueStats getRemovedChunksRecords() {
		return removedChunksRecords;
	}

	@JmxAttribute
	public ValueStats getAddedChunksRecords() {
		return addedChunksRecords;
	}

	@JmxAttribute
	public PromiseStats getPromiseConsolidate() {
		return promiseConsolidate;
	}

	@JmxAttribute
	public PromiseStats getPromiseConsolidateImpl() {
		return promiseConsolidateImpl;
	}

	@JmxAttribute
	public PromiseStats getPromiseCleanupIrrelevantChunks() {
		return promiseCleanupIrrelevantChunks;
	}

	@JmxAttribute
	public @Nullable Instant getLastNotEmptyConsolidationTime() {
		return lastNotEmptyConsolidationTimestamp == 0L ? null : Instant.ofEpochMilli(lastNotEmptyConsolidationTimestamp);
	}

	@JmxOperation
	public void consolidateNow() {
		consolidate();
	}

	@JmxOperation
	public void cleanupIrrelevantChunksNow() {
		cleanupIrrelevantChunks();
	}
}
