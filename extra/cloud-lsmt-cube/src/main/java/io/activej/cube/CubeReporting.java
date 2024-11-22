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

import io.activej.codegen.DefiningClassLoader;
import io.activej.cube.CubeState.CompatibleAggregations;
import io.activej.cube.CubeStructure.PreprocessedQuery;
import io.activej.cube.aggregation.predicate.AggregationPredicate;
import io.activej.cube.exception.QueryException;
import io.activej.cube.ot.CubeDiff;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.etl.LogDiff;
import io.activej.etl.LogState;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.ot.StateManager;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.activej.common.collection.CollectorUtils.entriesToLinkedHashMap;
import static io.activej.reactor.Reactive.checkInReactorThread;

/**
 * Represents an OLAP cube. Provides methods for loading and querying data.
 * Also provides functionality for managing aggregations.
 */
public final class CubeReporting extends AbstractReactive
	implements ICubeReporting {

	private final StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager;
	private final CubeStructure structure;
	private final CubeExecutor executor;

	private CubeReporting(StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager, CubeStructure structure, CubeExecutor executor) {
		super(executor.getReactor());
		this.stateManager = stateManager;
		this.structure = structure;
		this.executor = executor;
	}

	public static CubeReporting create(StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateFunction, CubeStructure structure, CubeExecutor executor) {
		return new CubeReporting(stateFunction, structure, executor);
	}

	/**
	 * Returns a {@link StreamSupplier} of the records retrieved from cube for the specified query.
	 *
	 * @param <T>         type of output objects
	 * @param resultClass class of output records
	 * @return supplier that streams query results
	 */
	public <T> StreamSupplier<T> queryRawStream(
		List<String> dimensions, List<String> storedMeasures, AggregationPredicate where, Class<T> resultClass
	) {
		return queryRawStream(dimensions, storedMeasures, where, resultClass, executor.getClassLoader());
	}

	public <T> StreamSupplier<T> queryRawStream(
		List<String> dimensions, List<String> storedMeasures, AggregationPredicate where, Class<T> resultClass,
		DefiningClassLoader queryClassLoader
	) {
		List<CompatibleAggregations> compatibleAggregations = stateManager.query(state ->
			state.getDataState().findCompatibleAggregations(dimensions, storedMeasures, where)
		);

		return executor.queryRawStream(compatibleAggregations, dimensions, storedMeasures, where, resultClass, queryClassLoader);
	}

	@Override
	public Promise<QueryResult> query(CubeQuery cubeQuery) throws QueryException {
		checkInReactorThread(this);

		PreprocessedQuery preprocessedQuery = structure.preprocessQuery(cubeQuery);

		List<CompatibleAggregations> compatibleAggregations = stateManager.query(state ->
			state.getDataState().findCompatibleAggregations(
				new ArrayList<>(preprocessedQuery.resultDimensions()),
				new ArrayList<>(preprocessedQuery.resultStoredMeasures()),
				cubeQuery.getWhere()
			));

		return executor.query(compatibleAggregations, preprocessedQuery);
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
