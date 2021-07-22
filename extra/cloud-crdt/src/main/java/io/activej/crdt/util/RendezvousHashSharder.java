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

package io.activej.crdt.util;

import io.activej.common.ApplicationSettings;
import io.activej.common.HashUtils;
import io.activej.common.ref.RefInt;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static io.activej.common.Checks.checkArgument;
import static java.util.stream.Collectors.toList;

public final class RendezvousHashSharder {
	public static final int NUMBER_OF_BUCKETS = ApplicationSettings.getInt(RendezvousHashSharder.class, "numberOfBuckets", 1024);

	static {
		checkArgument((NUMBER_OF_BUCKETS & (NUMBER_OF_BUCKETS - 1)) == 0, "Number of buckets must be a power of two");
	}

	private final List<ObjWithIndex> totalPartitions;
	private final int[][] buckets;
	private final int topShards;

	private int @Nullable [] predefined;

	private RendezvousHashSharder(List<ObjWithIndex> totalPartitions, int topShards) {
		this.totalPartitions = totalPartitions;
		this.topShards = topShards;
		this.buckets = new int[NUMBER_OF_BUCKETS][topShards];
	}

	public static RendezvousHashSharder create(Set<? extends Comparable<?>> partitionIds, int topShards) {
		RefInt indexRef = new RefInt(0);
		List<ObjWithIndex> totalPartitions = partitionIds.stream()
				.sorted()
				.map(partitionId -> new ObjWithIndex(partitionId, indexRef.value++))
				.collect(toList());

		RendezvousHashSharder sharder = new RendezvousHashSharder(totalPartitions, topShards);
		sharder.recompute();
		return sharder;
	}

	public void recompute(Set<? extends Comparable<?>> partitionIds) {
		if (!totalPartitions.removeIf(el -> !partitionIds.contains(el.object))) {
			return;
		}
		recompute();
	}

	private void recompute() {
		if (totalPartitions.size() <= topShards) {
			predefined = new int[totalPartitions.size()];
			for (int i = 0; i < totalPartitions.size(); i++) {
				predefined[i] = totalPartitions.get(i).index;
			}
		} else {
			refillBuckets();
		}
	}

	private void refillBuckets() {
		for (int n = 0; n < buckets.length; n++) {
			int finalN = n;

			buckets[n] = totalPartitions.stream()
					.sorted(Comparator.<ObjWithIndex>comparingInt(x -> HashUtils.murmur3hash(x.object.hashCode(), finalN)).reversed())
					.mapToInt(value -> value.index)
					.limit(topShards)
					.toArray();
		}
	}

	public int[] shard(Object key) {
		if (predefined != null) return predefined;

		return buckets[key.hashCode() & (NUMBER_OF_BUCKETS - 1)];
	}

	public int indexOf(Comparable<?> partition){
		for (ObjWithIndex objWithIndex : totalPartitions) {
			if (objWithIndex.object.equals(partition)){
				return objWithIndex.index;
			}
		}
		throw new NoSuchElementException("Cannot find partition: " + partition);
	}

	private static final class ObjWithIndex {
		final Comparable<?> object;
		final int index;

		ObjWithIndex(Comparable<?> object, int index) {
			this.object = object;
			this.index = index;
		}
	}
}
