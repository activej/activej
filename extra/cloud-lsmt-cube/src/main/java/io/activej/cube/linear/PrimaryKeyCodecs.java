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

import io.activej.aggregation.Aggregation;
import io.activej.aggregation.PrimaryKey;
import io.activej.codec.StructuredCodec;
import io.activej.cube.Cube;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static io.activej.aggregation.util.Utils.getPrimaryKeyCodec;

public final class PrimaryKeyCodecs {
	private final Function<String, @Nullable StructuredCodec<PrimaryKey>> lookUpFn;

	private PrimaryKeyCodecs(Function<String, @Nullable StructuredCodec<PrimaryKey>> lookUpFn) {
		this.lookUpFn = lookUpFn;
	}

	public static PrimaryKeyCodecs ofCube(Cube cube) {
		Map<String, StructuredCodec<PrimaryKey>> codecMap = new HashMap<>();
		for (String aggregationId : cube.getAggregationIds()) {
			Aggregation aggregation = cube.getAggregation(aggregationId);
			codecMap.put(aggregationId, getPrimaryKeyCodec(aggregation.getStructure()));
		}
		return new PrimaryKeyCodecs(codecMap::get);
	}

	public static PrimaryKeyCodecs ofLookUp(Function<String, @Nullable StructuredCodec<PrimaryKey>> lookUpFn) {
		return new PrimaryKeyCodecs(lookUpFn);
	}

	public StructuredCodec<PrimaryKey> getCodec(String aggregationId) {
		return lookUpFn.apply(aggregationId);
	}
}
