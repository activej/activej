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

package io.activej.cube.ot;

import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.ot.AggregationDiff;

import java.util.*;
import java.util.stream.LongStream;

public final class CubeDiff {
	private final Map<String, AggregationDiff> diffs;

	private CubeDiff(Map<String, AggregationDiff> diffs) {
		this.diffs = diffs;
	}

	public static CubeDiff of(Map<String, AggregationDiff> aggregationOps) {
		Map<String, AggregationDiff> map = new HashMap<>();
		for (Map.Entry<String, AggregationDiff> entry : aggregationOps.entrySet()) {
			AggregationDiff value = entry.getValue();
			if (!value.isEmpty()) {
				map.put(entry.getKey(), value);
			}
		}
		return new CubeDiff(map);
	}

	public static CubeDiff reduce(List<CubeDiff> cubeDiffs) {
		Map<String, AggregationDiff> diffs = new LinkedHashMap<>();
		for (var cubeDiff : cubeDiffs) {
			for (var entry : cubeDiff.entrySet()) {
				diffs.compute(entry.getKey(),
					(aggregationId, aggregationDiff) ->
						aggregationDiff == null ?
							entry.getValue() :
							AggregationDiff.squash(aggregationDiff, entry.getValue())
				);
			}
		}
		return CubeDiff.of(diffs);
	}

	public Map<String, AggregationDiff> getDiffs() {
		return diffs;
	}

	public Set<String> keySet() {
		return diffs.keySet();
	}

	public Set<Map.Entry<String, AggregationDiff>> entrySet() {
		return diffs.entrySet();
	}

	public AggregationDiff get(String id) {
		return diffs.get(id);
	}

	public static CubeDiff empty() {
		return new CubeDiff(Map.of());
	}

	public CubeDiff inverse() {
		Map<String, AggregationDiff> map = new HashMap<>();
		for (Map.Entry<String, AggregationDiff> entry : diffs.entrySet()) {
			String key = entry.getKey();
			AggregationDiff value = entry.getValue();
			map.put(key, value.inverse());
		}
		return new CubeDiff(map);
	}

	public boolean isEmpty() {
		return diffs.isEmpty();
	}

	public LongStream addedChunks() {
		return diffs.values().stream()
			.flatMap(aggregationDiff -> aggregationDiff.getAddedChunks().stream())
			.mapToLong(AggregationChunk::getChunkId);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		CubeDiff cubeDiff = (CubeDiff) o;

		return Objects.equals(diffs, cubeDiff.diffs);
	}

	@Override
	public int hashCode() {
		return diffs != null ? diffs.hashCode() : 0;
	}

	@Override
	public String toString() {
		return "{diffs:" + diffs.size() + '}';
	}
}
