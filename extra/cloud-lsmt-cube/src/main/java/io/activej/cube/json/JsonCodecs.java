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

import io.activej.cube.AggregationStructure;
import io.activej.cube.CubeStructure;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.ot.CubeDiff;
import io.activej.json.JsonCodec;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.activej.cube.aggregation.json.JsonCodecs.ofAggregationDiff;
import static io.activej.json.JsonCodecs.ofMap;

public class JsonCodecs {

	public static JsonCodec<CubeDiff> ofCubeDiff(CubeStructure cubeStructure) {
		Map<String, JsonCodec<AggregationDiff>> aggregationDiffCodecs = new LinkedHashMap<>();

		for (String aggregationId : cubeStructure.getAggregationIds()) {
			AggregationStructure aggregationStructure = cubeStructure.getAggregationStructure(aggregationId);
			JsonCodec<AggregationDiff> aggregationDiffCodec = ofAggregationDiff(aggregationStructure);
			aggregationDiffCodecs.put(aggregationId, aggregationDiffCodec);
		}

		return ofMap(aggregationDiffCodecs::get).transform(CubeDiff::getDiffs, CubeDiff::of);
	}

}
