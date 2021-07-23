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

package io.activej.ot;

import io.activej.ot.OTCommitFactory.DiffsWithLevel;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.activej.common.Checks.checkState;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;

public final class OTCommit<K, D> {
	public static final int INITIAL_EPOCH = 0;
	private final int epoch;
	private final K id;
	private final Map<K, DiffsWithLevel<D>> parentsWithLevels;
	private final Map<K, List<D>> parents;
	private final long level;

	private long timestamp;
	@Nullable
	private byte[] serializedData;

	private OTCommit(int epoch, K id, Map<K, DiffsWithLevel<D>> parents) {
		this.epoch = epoch;
		this.id = id;
		this.parentsWithLevels = parents;
        this.parents = parentsWithLevels.entrySet()
				.stream()
				.collect(toMap(Map.Entry::getKey, entry -> entry.getValue().getDiffs(), (u, v) -> {
                    throw new IllegalStateException("Duplicate key " + u);
                }, LinkedHashMap::new));
		this.level = parents.values().stream().mapToLong(DiffsWithLevel::getLevel).max().orElse(0L) + 1L;
	}

	public static <K, D> OTCommit<K, D> ofRoot(@NotNull K id) {
		return new OTCommit<>(INITIAL_EPOCH, id, emptyMap());
	}

	public static <K, D> OTCommit<K, D> of(int epoch, @NotNull K id, @NotNull Map<K, DiffsWithLevel<D>> parents) {
		return new OTCommit<>(epoch, id, parents);
	}

	public static <K, D> OTCommit<K, D> of(int epoch, K id, Set<K> parents, Function<K, List<D>> diffs, Function<K, Long> levels) {
        return of(epoch, id, parents.stream()
				.collect(toMap(
						parent -> parent,
						parent -> new DiffsWithLevel<>(levels.apply(parent), diffs.apply(parent)),
                        (u, v) -> { throw new IllegalStateException("Duplicate key " + u); },
						LinkedHashMap::new)));
	}

	public static <K, D> OTCommit<K, D> ofCommit(int epoch, @NotNull K id, @NotNull K parent, @NotNull DiffsWithLevel<D> diffs) {
		return new OTCommit<>(epoch, id, singletonMap(parent, diffs));
	}

	public static <K, D> OTCommit<K, D> ofCommit(int epoch, K id, K parent, List<D> diffs, long level) {
		return ofCommit(epoch, id, parent, new DiffsWithLevel<>(level, diffs));
	}

	public OTCommit<K, D> withTimestamp(long timestamp) {
		this.timestamp = timestamp;
		return this;
	}

	public OTCommit<K, D> withSerializedData(byte[] serializedData) {
		this.serializedData = serializedData;
		return this;
	}

	public boolean isRoot() {
		return parents.isEmpty();
	}

	public boolean isMerge() {
		return parents.size() > 1;
	}

	public boolean isCommit() {
		return parents.size() == 1;
	}

	public long getLevel() {
		return level;
	}

	public K getId() {
		return id;
	}

	public int getEpoch() {
		return epoch;
	}

	public Map<K, List<D>> getParents() {
		return parents;
	}

	public Map<K, DiffsWithLevel<D>> getParentsWithLevels() {
		return parentsWithLevels;
	}

	public Set<K> getParentIds() {
		return parents.keySet();
	}

	public long getTimestamp() {
		return timestamp;
	}

	public Instant getInstant() {
		checkState(timestamp != 0L, "Timestamp has not been set");
		return Instant.ofEpochMilli(timestamp);
	}

	@Nullable
	public byte[] getSerializedData() {
		return serializedData;
	}

	public void setSerializedData(@Nullable byte[] serializedData) {
		this.serializedData = serializedData;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		OTCommit<?, ?> commit = (OTCommit<?, ?>) o;
		return id.equals(commit.id);
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public String toString() {
		return "{id=" + id + ", parents=" + getParentIds() + "}";
	}
}
