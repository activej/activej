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

import io.activej.aggregation.AggregationChunk;
import io.activej.aggregation.AggregationChunkStorage;
import io.activej.aggregation.ot.AggregationDiff;
import io.activej.async.function.AsyncPredicate;
import io.activej.async.function.AsyncSupplier;
import io.activej.common.initializer.WithInitializer;
import io.activej.cube.ReactiveCube;
import io.activej.cube.exception.CubeException;
import io.activej.cube.ot.CubeDiff;
import io.activej.etl.LogDiff;
import io.activej.etl.LogOTProcessor;
import io.activej.etl.LogOTState;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.stats.ValueStats;
import io.activej.ot.OTStateManager;
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
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public final class CubeLogProcessorController<K, C> extends AbstractReactive
		implements ReactiveJmxBeanWithStats, WithInitializer<CubeLogProcessorController<K, C>> {
	private static final Logger logger = LoggerFactory.getLogger(CubeLogProcessorController.class);

	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final List<LogOTProcessor<?, CubeDiff>> logProcessors;
	private final AggregationChunkStorage<C> chunkStorage;
	private final OTStateManager<K, LogDiff<CubeDiff>> stateManager;
	private final AsyncPredicate<K> predicate;

	private boolean parallelRunner;

	private final PromiseStats promiseProcessLogs = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseProcessLogsImpl = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final ValueStats addedChunks = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final ValueStats addedChunksRecords = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withRate();

	CubeLogProcessorController(Reactor reactor,
			List<LogOTProcessor<?, CubeDiff>> logProcessors, AggregationChunkStorage<C> chunkStorage, OTStateManager<K, LogDiff<CubeDiff>> stateManager, AsyncPredicate<K> predicate) {
		super(reactor);
		this.logProcessors = logProcessors;
		this.chunkStorage = chunkStorage;
		this.stateManager = stateManager;
		this.predicate = predicate;
	}

	public static <K, C> CubeLogProcessorController<K, C> create(Reactor reactor,
			LogOTState<CubeDiff> state,
			OTStateManager<K, LogDiff<CubeDiff>> stateManager,
			AggregationChunkStorage<C> chunkStorage,
			List<LogOTProcessor<?, CubeDiff>> logProcessors) {
		ReactiveCube cube = (ReactiveCube) state.getDataState();
		AsyncPredicate<K> predicate = AsyncPredicate.of(commitId -> {
			if (cube.containsExcessiveNumberOfOverlappingChunks()) {
				logger.info("Cube contains excessive number of overlapping chunks");
				return false;
			}
			return true;
		});
		return new CubeLogProcessorController<>(reactor, logProcessors, chunkStorage, stateManager, predicate);
	}

	public CubeLogProcessorController<K, C> withParallelRunner(boolean parallelRunner) {
		this.parallelRunner = parallelRunner;
		return this;
	}

	private final AsyncSupplier<Boolean> processLogs = coalesce(this::doProcessLogs);

	public Promise<Boolean> processLogs() {
		return processLogs.get();
	}

	Promise<Boolean> doProcessLogs() {
		return process()
				.whenComplete(promiseProcessLogs.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), stateManager));
	}

	Promise<Boolean> process() {
		return Promise.complete()
				.then(stateManager::sync)
				.mapException(e -> new CubeException("Failed to synchronize state prior to log processing", e))
				.map($ -> stateManager.getCommitId())
				.then(commitId -> predicate.test(commitId)
						.mapException(e -> new CubeException("Failed to test commit '" + commitId + "' with predicate", e)))
				.thenIfElse(ok -> ok,
						$ -> {
							logger.info("Pull to commit: {}, start log processing", stateManager.getCommitId());

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
											.whenResult(() -> stateManager.addAll(diffs))
											.then(() -> stateManager.sync()
													.mapException(e -> new CubeException("Failed to synchronize state after log processing, resetting", e)))
											.whenException(e -> stateManager.reset())
											.map($2 -> true));
						},
						$ -> Promise.of(false))
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

	@JmxOperation
	public void processLogsNow() {
		processLogs();
	}

}
