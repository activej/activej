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

package io.activej.aggregation.ot;

import io.activej.aggregation.AggregationChunk;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import static io.activej.common.Utils.union;
import static java.util.Collections.emptySet;

public final class AggregationDiff {
	private final Set<AggregationChunk> addedChunks;
	private final Set<AggregationChunk> removedChunks;

	private AggregationDiff(Set<AggregationChunk> addedChunks, Set<AggregationChunk> removedChunks) {
		this.addedChunks = addedChunks;
		this.removedChunks = removedChunks;
	}

	public static AggregationDiff of(@NotNull Set<AggregationChunk> addedChunks, @NotNull Set<AggregationChunk> removedChunks) {
		return new AggregationDiff(addedChunks, removedChunks);
	}

	public static AggregationDiff of(Set<AggregationChunk> addedChunks) {
		return of(addedChunks, emptySet());
	}

	public static AggregationDiff empty() {
		return new AggregationDiff(emptySet(), emptySet());
	}

	public Set<AggregationChunk> getAddedChunks() {
		return addedChunks;
	}

	public Set<AggregationChunk> getRemovedChunks() {
		return removedChunks;
	}

	public AggregationDiff inverse() {
		return new AggregationDiff(removedChunks, addedChunks);
	}

	public boolean isEmpty() {
		return addedChunks.isEmpty() && removedChunks.isEmpty();
	}

	public static AggregationDiff squash(AggregationDiff commit1, AggregationDiff commit2) {
		Set<AggregationChunk> addedChunks1 = new LinkedHashSet<>(commit1.addedChunks);
		addedChunks1.removeAll(commit2.removedChunks);
		Set<AggregationChunk> addedChunks2 = new LinkedHashSet<>(commit2.addedChunks);
		addedChunks2.removeAll(commit1.removedChunks);
		Set<AggregationChunk> addedChunks = union(addedChunks1, addedChunks2);

		Set<AggregationChunk> removedChunks1 = new LinkedHashSet<>(commit1.removedChunks);
		removedChunks1.removeAll(commit2.addedChunks);
		Set<AggregationChunk> removedChunks2 = new LinkedHashSet<>(commit2.removedChunks);
		removedChunks2.removeAll(commit1.addedChunks);
		Set<AggregationChunk> removedChunks = union(removedChunks1, removedChunks2);

		return AggregationDiff.of(addedChunks, removedChunks);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		AggregationDiff that = (AggregationDiff) o;

		if (!Objects.equals(addedChunks, that.addedChunks)) return false;
		return Objects.equals(removedChunks, that.removedChunks);
	}

	@Override
	public int hashCode() {
		int result = addedChunks != null ? addedChunks.hashCode() : 0;
		result = 31 * result + (removedChunks != null ? removedChunks.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "{addedChunks:" + addedChunks.size() + ", removedChunks:" + removedChunks.size() + '}';
	}
}
