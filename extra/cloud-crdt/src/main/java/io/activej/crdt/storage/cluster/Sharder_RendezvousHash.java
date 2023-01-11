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

package io.activej.crdt.storage.cluster;

import io.activej.common.ApplicationSettings;
import io.activej.common.HashUtils;
import io.activej.common.initializer.WithInitializer;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.ToIntFunction;

import static io.activej.common.Checks.checkArgument;

public final class Sharder_RendezvousHash<K> implements Sharder<K>, WithInitializer<Sharder_RendezvousHash<K>> {
	public static final int NUMBER_OF_BUCKETS = ApplicationSettings.getInt(Sharder_RendezvousHash.class, "numberOfBuckets", 512);

	static {
		checkArgument((NUMBER_OF_BUCKETS & (NUMBER_OF_BUCKETS - 1)) == 0, "Number of buckets must be a power of two");
	}

	final int[][] buckets;
	final ToIntFunction<K> keyHashFn;

	Sharder_RendezvousHash(int[][] buckets, ToIntFunction<K> keyHashFn) {
		this.buckets = buckets;
		this.keyHashFn = keyHashFn;
	}

	public static <K extends Comparable<K>, P> Sharder_RendezvousHash<K> create(
			ToIntFunction<K> keyHashFn, ToIntFunction<P> partitionIdHashCode,
			Set<P> partitions, List<P> partitionsAlive, int shards, boolean repartition) {
		Map<P, Integer> partitionsAliveMap = new HashMap<>();
		for (P partitionId : partitionsAlive) {
			partitionsAliveMap.put(partitionId, partitionsAliveMap.size());
		}

		int[][] buckets = new int[NUMBER_OF_BUCKETS][];

		//noinspection NullableProblems
		class ObjWithIndex {
			final P partitionId;
			final @Nullable Integer aliveIndex;
			private long hash;

			public long getHash() {
				return hash;
			}

			ObjWithIndex(P partitionId, Integer aliveIndex) {
				this.partitionId = partitionId;
				this.aliveIndex = aliveIndex;
			}
		}

		ObjWithIndex[] toSort = new ObjWithIndex[partitions.size()];
		int i = 0;
		for (P partitionId : partitions) {
			toSort[i++] = new ObjWithIndex(partitionId, partitionsAliveMap.get(partitionId));
		}
		int[] buf = new int[partitions.size()];

		for (int bucket = 0; bucket < buckets.length; bucket++) {
			for (ObjWithIndex obj : toSort) {
				obj.hash = hashBucket(partitionIdHashCode.applyAsInt(obj.partitionId), bucket);
			}

			Arrays.sort(toSort, Comparator.comparingLong(ObjWithIndex::getHash).reversed());

			int n = 0;
			int shardsN = shards;
			for (ObjWithIndex obj : toSort) {
				if (shardsN <= 0) break;
				if (obj.aliveIndex != null) {
					buf[n++] = obj.aliveIndex;
					shardsN--;
				} else if (!repartition) {
					shardsN--;
				}
			}

			buckets[bucket] = Arrays.copyOf(buf, n);
			Arrays.sort(buckets[bucket]);
		}
		return new Sharder_RendezvousHash<>(buckets, keyHashFn);
	}

	static <K> Sharder<K> unionOf(List<Sharder_RendezvousHash<K>> sharders) {
		if (sharders.isEmpty()) return Sharder.none();
		if (sharders.size() == 1) return sharders.get(0);
		int[][] buckets = new int[sharders.get(0).buckets.length][];
		ToIntFunction<K> keyHashFn = sharders.get(0).keyHashFn;
		int[] buf = new int[0];
		for (int bucket = 0; bucket < buckets.length; bucket++) {
			int pos = 0;
			for (Sharder_RendezvousHash<K> sharder : sharders) {
				int[] selected = sharder.buckets[bucket];
				NEXT:
				for (int idx : selected) {
					for (int i = 0; i < pos; i++) {
						if (idx == buf[i]) continue NEXT;
					}
					if (buf.length <= pos) buf = Arrays.copyOf(buf, buf.length * 2 + 1);
					buf[pos++] = idx;
				}
			}
			buckets[bucket] = Arrays.copyOf(buf, pos);
		}
		return new Sharder_RendezvousHash<>(buckets, keyHashFn);
	}

	@Override
	public int[] shard(K key) {
		return buckets[keyHashFn.applyAsInt(key) & (NUMBER_OF_BUCKETS - 1)];
	}

	public static long hashBucket(int partitionHash, int bucket) {
		return HashUtils.murmur3hash(((long) partitionHash << 32) | (bucket & 0xFFFFFFFFL));
	}
}
