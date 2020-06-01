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

package io.activej.crdt;

import io.activej.common.HashUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static io.activej.common.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public final class RendezvousHashSharder<I, K> {

	private int numOfBuckets = 1024;
	private int[][] buckets;

	private RendezvousHashSharder() {
	}

	public static <I, K> RendezvousHashSharder<I, K> create(List<I> partitionIds, int topShards) {
		RendezvousHashSharder<I, K> sharder = new RendezvousHashSharder<>();
		sharder.recompute(partitionIds, topShards);
		return sharder;
	}

	public RendezvousHashSharder<I, K> withNumberOfBuckets(int numOfBuckets) {
		checkArgument((numOfBuckets & (numOfBuckets - 1)) == 0, "Number of buckets must be a power of two");

		this.numOfBuckets = numOfBuckets;
		return this;
	}

	public void recompute(List<I> partitionIds, int topShards) {
		checkArgument(topShards > 0, "Top number of partitions must be positive");
		checkArgument(topShards <= partitionIds.size(), "Top number of partitions must less than or equal to number of partitions");

		buckets = new int[numOfBuckets][topShards];

		class ObjWithIndex<O> {
			final O object;
			final int index;

			ObjWithIndex(O object, int index) {
				this.object = object;
				this.index = index;
			}
		}
		List<ObjWithIndex<I>> indexed = new ArrayList<>();
		for (int i = 0; i < partitionIds.size(); i++) {
			indexed.add(new ObjWithIndex<>(partitionIds.get(i), i));
		}

		for (int n = 0; n < buckets.length; n++) {
			int finalN = n;
			List<ObjWithIndex<I>> copy = indexed.stream()
					.sorted(Comparator.<ObjWithIndex<I>>comparingInt(x -> HashUtils.murmur3hash(x.object.hashCode(), finalN)).reversed())
					.collect(toList());

			for (int i = 0; i < topShards; i++) {
				buckets[n][i] = copy.get(i).index;
			}
		}
	}

	public int[] shard(K key) {
		return buckets[key.hashCode() & (numOfBuckets - 1)];
	}
}
