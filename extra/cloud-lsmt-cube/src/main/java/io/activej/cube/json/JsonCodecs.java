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

package io.activej.cube.json;

import io.activej.codegen.DefiningClassLoader;
import io.activej.cube.AggregationStructure;
import io.activej.cube.CubeStructure;
import io.activej.cube.QueryResult;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.aggregation.predicate.AggregationPredicate;
import io.activej.cube.ot.CubeDiff;
import io.activej.json.JsonCodec;
import io.activej.json.JsonCodecFactory;

import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.activej.common.collection.CollectorUtils.entriesToHashMap;
import static io.activej.cube.aggregation.json.JsonCodecs.ofAggregationDiff;
import static io.activej.json.JsonCodecs.ofMap;

public class JsonCodecs {

	public static JsonCodec<CubeDiff> createCubeDiffCodec(CubeStructure cubeStructure) {
		Map<String, JsonCodec<AggregationDiff>> aggregationDiffCodecs = new LinkedHashMap<>();

		for (String aggregationId : cubeStructure.getAggregationIds()) {
			AggregationStructure aggregationStructure = cubeStructure.getAggregationStructure(aggregationId);
			JsonCodec<AggregationDiff> aggregationDiffCodec = ofAggregationDiff(aggregationStructure);
			aggregationDiffCodecs.put(aggregationId, aggregationDiffCodec);
		}

		return ofMap(aggregationDiffCodecs::get).transform(CubeDiff::getDiffs, CubeDiff::of);
	}

	public static JsonCodec<AggregationPredicate> createAggregationPredicateCodec(
		JsonCodecFactory factory,
		CubeStructure structure
	) {
		return createAggregationPredicateCodec(factory, structure.getAllAttributeTypes(), structure.getAllMeasureTypes());
	}

	public static JsonCodec<AggregationPredicate> createAggregationPredicateCodec(
		JsonCodecFactory factory,
		Map<String, Type> attributeTypes, Map<String, Type> measureTypes
	) {
		Map<String, JsonCodec<Object>> attributeCodecs = new LinkedHashMap<>();
		for (Map.Entry<String, Type> entry : attributeTypes.entrySet()) {
			attributeCodecs.put(entry.getKey(), factory.resolve(entry.getValue()).nullable());
		}
		for (Map.Entry<String, Type> entry : measureTypes.entrySet()) {
			attributeCodecs.put(entry.getKey(), factory.resolve(entry.getValue()));
		}
		return new AggregationPredicateJsonCodec(attributeCodecs);
	}

	public static JsonCodec<QueryResult> createQueryResultCodec(
		DefiningClassLoader classLoader,
		JsonCodecFactory factory,
		CubeStructure structure
	) {
		return createQueryResultCodec(classLoader, factory, structure.getAllAttributeTypes(), structure.getAllMeasureTypes());
	}

	public static JsonCodec<QueryResult> createQueryResultCodec(
		DefiningClassLoader classLoader,
		JsonCodecFactory factory,
		Map<String, Type> attributeTypes,
		Map<String, Type> measureTypes
	) {
		Map<String, JsonCodec<Object>> attributeCodecs = attributeTypes.entrySet().stream()
			.collect(entriesToHashMap(value -> factory.resolve(value).nullable()));
		Map<String, JsonCodec<Object>> measureCodecs = measureTypes.entrySet().stream()
			.collect(entriesToHashMap(factory::resolve));

		return QueryResultJsonCodec.create(classLoader, attributeTypes, measureTypes, attributeCodecs, measureCodecs);
	}

}
