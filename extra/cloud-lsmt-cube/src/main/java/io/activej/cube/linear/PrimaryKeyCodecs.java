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

package io.activej.cube.linear;

import io.activej.aggregation.Aggregation_Reactive;
import io.activej.aggregation.JsonCodec_PrimaryKey;
import io.activej.aggregation.PrimaryKey;
import io.activej.aggregation.util.JsonCodec;
import io.activej.cube.Cube_Reactive;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public final class PrimaryKeyCodecs {
	private final Function<String, @Nullable JsonCodec<PrimaryKey>> lookUpFn;

	private PrimaryKeyCodecs(Function<String, @Nullable JsonCodec<PrimaryKey>> lookUpFn) {
		this.lookUpFn = lookUpFn;
	}

	public static PrimaryKeyCodecs ofCube(Cube_Reactive cube) {
		Map<String, JsonCodec<PrimaryKey>> codecMap = new HashMap<>();
		for (String aggregationId : cube.getAggregationIds()) {
			Aggregation_Reactive aggregation = cube.getAggregation(aggregationId);
			codecMap.put(aggregationId, JsonCodec_PrimaryKey.create(aggregation.getStructure()));
		}
		return new PrimaryKeyCodecs(codecMap::get);
	}

	public static PrimaryKeyCodecs ofLookUp(Function<String, @Nullable JsonCodec<PrimaryKey>> lookUpFn) {
		return new PrimaryKeyCodecs(lookUpFn);
	}

	public JsonCodec<PrimaryKey> getCodec(String aggregationId) {
		return lookUpFn.apply(aggregationId);
	}
}
