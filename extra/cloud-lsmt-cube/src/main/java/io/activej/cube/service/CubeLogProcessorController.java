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

import io.activej.async.function.AsyncSupplier;
import io.activej.common.builder.AbstractBuilder;
import io.activej.cube.CubeState;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.IAggregationChunkStorage;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.exception.CubeException;
import io.activej.cube.ot.CubeDiff;
import io.activej.etl.LogDiff;
import io.activej.etl.LogProcessor;
import io.activej.etl.LogState;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.stats.ValueStats;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.jmx.PromiseStats;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static io.activej.async.function.AsyncSuppliers.coalesce;
import static io.activej.async.util.LogUtils.thisMethod;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.promise.Promises.asPromises;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public final class CubeLogProcessorController<C> extends AbstractReactive
	implements ReactiveJmxBeanWithStats {

	public static final int DEFAULT_OVERLAPPING_CHUNKS_THRESHOLD = 300;

	private static final Logger logger = LoggerFactory.getLogger(CubeLogProcessorController.class);

	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final List<LogProcessor<?, CubeDiff>> logProcessors;
	private final IAggregationChunkStorage<C> chunkStorage;
	private final ServiceStateManager<LogDiff<CubeDiff>> stateManager;
	private AsyncSupplier<Boolean> predicate;

	private boolean parallelRunner;

	private final PromiseStats promiseProcessLogs = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseProcessLogsImpl = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final ValueStats addedChunks = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final ValueStats addedChunksRecords = ValueStats.builder(DEFAULT_SMOOTHING_WINDOW)
		.withRate()
		.build();

	private int maxOverlappingChunksToProcessLogs = DEFAULT_OVERLAPPING_CHUNKS_THRESHOLD;

	private CubeLogProcessorController(
		Reactor reactor, List<LogProcessor<?, CubeDiff>> logProcessors, IAggregationChunkStorage<C> chunkStorage,
		ServiceStateManager<LogDiff<CubeDiff>> stateManager
	) {
		super(reactor);
		this.logProcessors = logProcessors;
		this.chunkStorage = chunkStorage;
		this.stateManager = stateManager;
	}

	public static <C> CubeLogProcessorController<C> create(
		Reactor reactor, LogState<CubeDiff, CubeState> state, ServiceStateManager<LogDiff<CubeDiff>> stateManager,
		IAggregationChunkStorage<C> chunkStorage, List<LogProcessor<?, CubeDiff>> logProcessors
	) {
		return builder(reactor, state, stateManager, chunkStorage, logProcessors).build();
	}

	public static <C> CubeLogProcessorController<C>.Builder builder(
		Reactor reactor, LogState<CubeDiff, CubeState> state, ServiceStateManager<LogDiff<CubeDiff>> stateManager,
		IAggregationChunkStorage<C> chunkStorage, List<LogProcessor<?, CubeDiff>> logProcessors
	) {
		CubeState cubeState = state.getDataState();
		return new CubeLogProcessorController<>(reactor, logProcessors, chunkStorage, stateManager).new Builder(cubeState);
	}

	public final class Builder extends AbstractBuilder<Builder, CubeLogProcessorController<C>> {
		private final CubeState cubeState;

		private Builder(CubeState cubeState) {
			this.cubeState = cubeState;
		}

		public Builder withParallelRunner(boolean parallelRunner) {
			checkNotBuilt(this);
			CubeLogProcessorController.this.parallelRunner = parallelRunner;
			return this;
		}

		public Builder withMaxOverlappingChunksToProcessLogs(int maxOverlappingChunksToProcessLogs) {
			checkNotBuilt(this);
			CubeLogProcessorController.this.maxOverlappingChunksToProcessLogs = maxOverlappingChunksToProcessLogs;
			return this;
		}

		@Override
		protected CubeLogProcessorController<C> doBuild() {
			predicate = AsyncSupplier.of(() -> {
				if (cubeState.containsExcessiveNumberOfOverlappingChunks(maxOverlappingChunksToProcessLogs)) {
					logger.info("Cube contains excessive number of overlapping chunks");
					return false;
				}
				return true;
			});

			return CubeLogProcessorController.this;
		}
	}

	private final AsyncSupplier<Boolean> processLogs = coalesce(this::doProcessLogs);

	public Promise<Boolean> processLogs() {
		checkInReactorThread(this);
		return processLogs.get();
	}

	Promise<Boolean> doProcessLogs() {
		checkInReactorThread(this);
		return process()
			.whenComplete(promiseProcessLogs.recordStats())
			.whenComplete(toLogger(logger, thisMethod(), stateManager));
	}

	Promise<Boolean> process() {
		checkInReactorThread(this);
		return Promise.complete()
			.then(stateManager::sync)
			.mapException(e -> new CubeException("Failed to synchronize state prior to log processing", e))
			.then(() -> predicate.get()
				.mapException(e -> new CubeException("Failed to test cube with predicate", e)))
			.then(ok -> {
				if (!ok) return Promise.of(false);

				logger.info("Start log processing");

				List<AsyncSupplier<LogDiff<CubeDiff>>> tasks = logProcessors.stream()
					.map(logProcessor -> (AsyncSupplier<LogDiff<CubeDiff>>) logProcessor::processLog)
					.collect(toList());

				Promise<List<LogDiff<CubeDiff>>> promise = parallelRunner ?
					Promises.toList(tasks.stream().map(AsyncSupplier::get)) :
					Promises.reduce(toList(), 1, asPromises(tasks));

				return promise
					.mapException(e -> new CubeException("Failed to process logs", e))
					.whenComplete(promiseProcessLogsImpl.recordStats())
					.whenResult(this::cubeDiffJmx)
					.then(diffs -> chunkStorage.finish(addedChunks(diffs))
						.mapException(e -> new CubeException("Failed to finalize chunks in storage", e))
						.then(() -> stateManager.push(diffs)
							.mapException(e -> new CubeException("Failed to synchronize state after log processing, resetting", e)))
						.whenException(e -> stateManager.reset())
						.map($2 -> true));
			})
			.whenComplete(toLogger(logger, thisMethod(), stateManager));
	}

	private void cubeDiffJmx(List<LogDiff<CubeDiff>> logDiffs) {
		long curAddedChunks = 0;
		long curAddedChunksRecords = 0;

		for (LogDiff<CubeDiff> logDiff : logDiffs) {
			for (CubeDiff cubeDiff : logDiff.getDiffs()) {
				for (String key : cubeDiff.keySet()) {
					AggregationDiff aggregationDiff = cubeDiff.get(key);
					curAddedChunks += aggregationDiff.getAddedChunks().size();
					for (AggregationChunk aggregationChunk : aggregationDiff.getAddedChunks()) {
						curAddedChunksRecords += aggregationChunk.getCount();
					}
				}
			}
		}

		addedChunks.recordValue(curAddedChunks);
		addedChunksRecords.recordValue(curAddedChunksRecords);
	}

	@SuppressWarnings("unchecked")
	private Set<C> addedChunks(List<LogDiff<CubeDiff>> diffs) {
		return diffs.stream()
			.flatMap(LogDiff::diffs)
			.flatMap(CubeDiff::addedChunks)
			.map(id -> (C) id)
			.collect(toSet());
	}

	@JmxAttribute
	public ValueStats getLastAddedChunks() {
		return addedChunks;
	}

	@JmxAttribute
	public ValueStats getLastAddedChunksRecords() {
		return addedChunksRecords;
	}

	@JmxAttribute
	public PromiseStats getPromiseProcessLogs() {
		return promiseProcessLogs;
	}

	@JmxAttribute
	public PromiseStats getPromiseProcessLogsImpl() {
		return promiseProcessLogsImpl;
	}

	@JmxAttribute
	public boolean isParallelRunner() {
		return parallelRunner;
	}

	@JmxAttribute
	public void setParallelRunner(boolean parallelRunner) {
		this.parallelRunner = parallelRunner;
	}

	@JmxAttribute
	public int getMaxOverlappingChunksToProcessLogs() {
		return maxOverlappingChunksToProcessLogs;
	}

	@JmxAttribute
	public void setMaxOverlappingChunksToProcessLogs(int maxOverlappingChunksToProcessLogs) {
		this.maxOverlappingChunksToProcessLogs = maxOverlappingChunksToProcessLogs;
	}

	@JmxOperation
	public void processLogsNow() {
		processLogs();
	}

}
