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

package io.activej.aggregation;

import io.activej.common.initializer.WithInitializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.activej.aggregation.AggregationPredicates.*;
import static java.util.Collections.unmodifiableList;

public class AggregationChunk implements WithInitializer<AggregationChunk> {
	public static AggregationChunk create(Object chunkId,
			List<String> fields,
			PrimaryKey minPrimaryKey, PrimaryKey maxPrimaryKey,
			int count) {
		return new AggregationChunk(chunkId, fields, minPrimaryKey, maxPrimaryKey, count);
	}

	private final Object chunkId;
	private final List<String> measures;
	private final PrimaryKey minPrimaryKey;
	private final PrimaryKey maxPrimaryKey;
	private final int count;

	private AggregationChunk(Object chunkId,
			List<String> measures,
			PrimaryKey minPrimaryKey, PrimaryKey maxPrimaryKey,
			int count) {
		this.chunkId = chunkId;
		this.measures = measures;
		this.minPrimaryKey = minPrimaryKey;
		this.maxPrimaryKey = maxPrimaryKey;
		this.count = count;
	}

	public Object getChunkId() {
		return chunkId;
	}

	public List<String> getMeasures() {
		return unmodifiableList(measures);
	}

	public PrimaryKey getMinPrimaryKey() {
		return minPrimaryKey;
	}

	public PrimaryKey getMaxPrimaryKey() {
		return maxPrimaryKey;
	}

	public int getCount() {
		return count;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		AggregationChunk chunk = (AggregationChunk) o;
		return Objects.equals(chunkId, chunk.chunkId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(chunkId, measures, minPrimaryKey, maxPrimaryKey, count);
	}

	public PredicateDef toPredicate(List<String> primaryKey) {
		List<PredicateDef> predicates = new ArrayList<>();
		for (int i = 0; i < primaryKey.size(); i++) {
			String key = primaryKey.get(i);
			Object from = minPrimaryKey.get(i);
			Object to = maxPrimaryKey.get(i);
			if (from.equals(to)) {
				predicates.add(eq(key, from));
			} else {
				predicates.add(between(key, (Comparable<?>) from, (Comparable<?>) to));
			}
		}
		return and(predicates);
	}

	@Override
	public String toString() {
		return "{" +
				"id=" + chunkId +
				", measures=" + measures +
				", minKey=" + minPrimaryKey +
				", maxKey=" + maxPrimaryKey +
				", count=" + count +
				'}';
	}
}
