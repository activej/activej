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
import io.activej.common.function.StateQueryFunction;
import io.activej.cube.CubeState.CompatibleAggregations;
import io.activej.cube.CubeStructure.PreprocessedQuery;
import io.activej.cube.aggregation.predicate.AggregationPredicate;
import io.activej.cube.exception.QueryException;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;

import java.util.ArrayList;
import java.util.List;

import static io.activej.reactor.Reactive.checkInReactorThread;

/**
 * Represents an OLAP cube. Provides methods for loading and querying data.
 * Also provides functionality for managing aggregations.
 */
public final class CubeReporting extends AbstractReactive
	implements ICubeReporting {

	private final StateQueryFunction<CubeState> stateFunction;
	private final CubeStructure structure;
	private final CubeExecutor executor;

	private CubeReporting(StateQueryFunction<CubeState> stateFunction, CubeStructure structure, CubeExecutor executor) {
		super(executor.getReactor());
		this.stateFunction = stateFunction;
		this.structure = structure;
		this.executor = executor;
	}

	public static CubeReporting create(StateQueryFunction<CubeState> stateFunction, CubeStructure structure, CubeExecutor executor) {
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
		List<CompatibleAggregations> compatibleAggregations = stateFunction.query(state ->
			state.findCompatibleAggregations(dimensions, storedMeasures, where)
		);

		return executor.queryRawStream(compatibleAggregations, dimensions, storedMeasures, where, resultClass, queryClassLoader);
	}

	@Override
	public Promise<QueryResult> query(CubeQuery cubeQuery) throws QueryException {
		checkInReactorThread(this);

		PreprocessedQuery preprocessedQuery = structure.preprocessQuery(cubeQuery);

		List<CompatibleAggregations> compatibleAggregations = stateFunction.query(state ->
			state.findCompatibleAggregations(
				new ArrayList<>(preprocessedQuery.resultDimensions()),
				new ArrayList<>(preprocessedQuery.resultStoredMeasures()),
				cubeQuery.getWhere().simplify()
			));

		return executor.query(compatibleAggregations, preprocessedQuery);
	}

	public CubeStructure getStructure() {
		return structure;
	}

	public StateQueryFunction<CubeState> getStateFunction() {
		return stateFunction;
	}

	public CubeExecutor getExecutor() {
		return executor;
	}
}