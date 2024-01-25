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
import io.activej.cube.aggregation.IAggregationChunkStorage;
import io.activej.cube.aggregation.ProtoAggregationChunk;
import io.activej.cube.aggregation.ot.ProtoAggregationDiff;
import io.activej.cube.exception.CubeException;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.ProtoCubeDiff;
import io.activej.etl.LogDiff;
import io.activej.etl.LogProcessor;
import io.activej.etl.LogState;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.activej.async.function.AsyncSuppliers.coalesce;
import static io.activej.async.util.LogUtils.thisMethod;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.cube.aggregation.util.Utils.materializeProtoDiff;
import static io.activej.promise.Promises.asPromises;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.util.stream.Collectors.toList;

public final class CubeLogProcessorController extends AbstractReactive
	implements ReactiveJmxBeanWithStats {

	public static final int DEFAULT_OVERLAPPING_CHUNKS_THRESHOLD = 300;

	private static final Logger logger = LoggerFactory.getLogger(CubeLogProcessorController.class);

	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final List<LogProcessor<?, ProtoCubeDiff, CubeDiff>> logProcessors;
	private final IAggregationChunkStorage chunkStorage;
	private final StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager;
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
		Reactor reactor, List<LogProcessor<?, ProtoCubeDiff, CubeDiff>> logProcessors, IAggregationChunkStorage chunkStorage,
		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager
	) {
		super(reactor);
		this.logProcessors = logProcessors;
		this.chunkStorage = chunkStorage;
		this.stateManager = stateManager;
	}

	public static CubeLogProcessorController create(
		Reactor reactor, StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager,
		IAggregationChunkStorage chunkStorage, List<LogProcessor<?, ProtoCubeDiff, CubeDiff>> logProcessors
	) {
		return builder(reactor, stateManager, chunkStorage, logProcessors).build();
	}

	public static CubeLogProcessorController.Builder builder(
		Reactor reactor, StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager,
		IAggregationChunkStorage chunkStorage, List<LogProcessor<?, ProtoCubeDiff, CubeDiff>> logProcessors
	) {
		return new CubeLogProcessorController(reactor, logProcessors, chunkStorage, stateManager).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, CubeLogProcessorController> {

		private Builder() {}

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
		protected CubeLogProcessorController doBuild() {
			predicate = AsyncSupplier.of(() -> stateManager.query(state -> {
				if (state.getDataState().containsExcessiveNumberOfOverlappingChunks(maxOverlappingChunksToProcessLogs)) {
					logger.info("Cube contains excessive number of overlapping chunks");
					return false;
				}
				return true;
			}));

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
			.then(stateManager::catchUp)
			.mapException(e -> new CubeException("Failed to synchronize state prior to log processing", e))
			.then(() -> predicate.get()
				.mapException(e -> new CubeException("Failed to test cube with predicate", e)))
			.then(ok -> {
				if (!ok) return Promise.of(false);

				logger.info("Start log processing");

				List<AsyncSupplier<LogDiff<ProtoCubeDiff>>> tasks = logProcessors.stream()
					.map(logProcessor -> (AsyncSupplier<LogDiff<ProtoCubeDiff>>) logProcessor::processLog)
					.collect(toList());

				Promise<List<LogDiff<ProtoCubeDiff>>> promise = parallelRunner ?
					Promises.toList(tasks.stream().map(AsyncSupplier::get)) :
					Promises.reduce(toList(), 1, asPromises(tasks));

				return promise
					.mapException(e -> new CubeException("Failed to process logs", e))
					.whenComplete(promiseProcessLogsImpl.recordStats())
					.whenResult(this::cubeDiffJmx)
					.then(protoDiffs -> chunkStorage.finish(addedProtoChunks(protoDiffs))
						.mapException(e -> new CubeException("Failed to finalize chunks in storage", e))
						.then(chunkIds -> stateManager.push(materializeProtoDiff(protoDiffs, chunkIds))
							.mapException(e -> new CubeException("Failed to synchronize state after log processing, resetting", e)))
						.map($ -> true));
			})
			.whenComplete(toLogger(logger, thisMethod(), stateManager));
	}

	private void cubeDiffJmx(List<LogDiff<ProtoCubeDiff>> logDiffs) {
		long curAddedChunks = 0;
		long curAddedChunksRecords = 0;

		for (LogDiff<ProtoCubeDiff> logDiff : logDiffs) {
			for (ProtoCubeDiff protoCubeDiff : logDiff.getDiffs()) {
				Map<String, ProtoAggregationDiff> diffs = protoCubeDiff.diffs();
				for (String key : diffs.keySet()) {
					ProtoAggregationDiff aggregationDiff = diffs.get(key);
					curAddedChunks += aggregationDiff.addedChunks().size();
					for (ProtoAggregationChunk aggregationChunk : aggregationDiff.addedChunks()) {
						curAddedChunksRecords += aggregationChunk.count();
					}
				}
			}
		}

		addedChunks.recordValue(curAddedChunks);
		addedChunksRecords.recordValue(curAddedChunksRecords);
	}

	private Set<String> addedProtoChunks(List<LogDiff<ProtoCubeDiff>> diffs) {
		return diffs.stream()
			.flatMap(LogDiff::diffs)
			.flatMap(ProtoCubeDiff::addedProtoChunks)
			.collect(Collectors.toSet());
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
