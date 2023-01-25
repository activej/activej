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

import io.activej.common.builder.AbstractBuilder;
import io.activej.ot.IOTCommitFactory.DiffsWithLevel;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import static io.activej.common.Checks.checkState;
import static io.activej.common.Utils.entriesToMap;
import static io.activej.common.Utils.keysToMap;

public final class OTCommit<K, D> {
	public static final int INITIAL_EPOCH = 0;
	private final int epoch;
	private final K id;
	private final Map<K, DiffsWithLevel<D>> parentsWithLevels;
	private final Map<K, List<D>> parents;
	private final long level;

	private long timestamp;
	private byte @Nullable [] serializedData;

	private OTCommit(int epoch, K id, Map<K, DiffsWithLevel<D>> parents) {
		this.epoch = epoch;
		this.id = id;
		this.parentsWithLevels = parents;
		this.parents = entriesToMap(parentsWithLevels.entrySet().stream(), DiffsWithLevel::getDiffs);
		this.level = parents.values().stream().mapToLong(DiffsWithLevel::getLevel).max().orElse(0L) + 1L;
	}

	public static <K, D> OTCommit<K, D> ofRoot(K id) {
		return new OTCommit<>(INITIAL_EPOCH, id, Map.of());
	}

	public static <K, D> OTCommit<K, D> of(int epoch, K id, Map<K, DiffsWithLevel<D>> parents) {
		return builder(epoch, id, parents).build();
	}

	public static <K, D> OTCommit<K, D> of(int epoch, K id, Set<K> parents, Function<K, List<D>> diffs, ToLongFunction<K> levels) {
		return of(epoch, id, keysToMap(parents.stream(), parent -> new DiffsWithLevel<>(levels.applyAsLong(parent), diffs.apply(parent))));
	}

	public static <K, D> OTCommit<K, D> ofCommit(int epoch, K id, K parent, DiffsWithLevel<D> diffs) {
		return builder(epoch, id, Map.of(parent, diffs)).build();
	}

	public static <K, D> OTCommit<K, D> ofCommit(int epoch, K id, K parent, List<D> diffs, long level) {
		return ofCommit(epoch, id, parent, new DiffsWithLevel<>(level, diffs));
	}

	public static <K, D> OTCommit<K, D>.Builder builder(int epoch, K id, Map<K, DiffsWithLevel<D>> parents) {
		return new OTCommit<>(epoch, id, parents).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, OTCommit<K, D>>{
		private Builder() {}

		public Builder withTimestamp(long timestamp) {
			checkNotBuilt(this);
			OTCommit.this.timestamp = timestamp;
			return this;
		}

		public Builder withSerializedData(byte[] serializedData) {
			checkNotBuilt(this);
			OTCommit.this.serializedData = serializedData;
			return this;
		}

		@Override
		protected OTCommit<K, D> doBuild() {
			return OTCommit.this;
		}
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

	public byte @Nullable [] getSerializedData() {
		return serializedData;
	}

	public void setSerializedData(byte @Nullable [] serializedData) {
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
