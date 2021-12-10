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

package io.activej.fs.cluster;

import java.util.List;
import java.util.Set;

import static io.activej.common.HashUtils.murmur3hash;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.toList;

/**
 * Strategy interface, which decides what file goes on which server from given ones.
 */
@FunctionalInterface
public interface ServerSelector {
	/**
	 * Implementation of rendezvous hash sharding algorithm
	 */
	ServerSelector RENDEZVOUS_HASH_SHARDER = (fileName, shards) -> {
		class ShardWithHash {
			private final Object shard;
			private final int hash;

			private ShardWithHash(Object shard, int hash) {
				this.shard = shard;
				this.hash = hash;
			}
		}
		return shards.stream()
				.map(shard -> new ShardWithHash(shard,
						(int) murmur3hash(((long) fileName.hashCode() << 32) | (shard.hashCode() & 0xFFFFFFFFL))))
				.sorted(comparingInt(h -> h.hash))
				.map(h -> h.shard)
				.collect(toList());
	};

	/**
	 * Selects partitions where given file should belong.
	 *
	 * @param fileName  name of the file
	 * @param shards    set of partition ids to choose from
	 * @return list of keys of servers ordered by priority where file with given name should be
	 */
	List<Object> selectFrom(String fileName, Set<Object> shards);
}
