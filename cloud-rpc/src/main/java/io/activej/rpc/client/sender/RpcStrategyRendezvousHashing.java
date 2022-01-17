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

package io.activej.rpc.client.sender;

import io.activej.async.callback.Callback;
import io.activej.common.HashUtils;
import io.activej.rpc.client.RpcClientConnectionPool;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.ToIntFunction;
import java.util.function.ToLongBiFunction;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.first;

public final class RpcStrategyRendezvousHashing implements RpcStrategy {
	private static final int MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT = 1;
	private static final int DEFAULT_BUCKET_CAPACITY = 2048;
	private static final ToLongBiFunction<Object, Integer> DEFAULT_BUCKET_HASH_FUNCTION = (shardId, bucketN) ->
			(int) HashUtils.murmur3hash(((long) shardId.hashCode() << 32) | (bucketN & 0xFFFFFFFFL));

	private final Map<Object, RpcStrategy> shards;
	private final ToIntFunction<?> hashFunction;
	private final int minShards;
	private final ToLongBiFunction<?, Integer> hashBucketFunction;
	private final int buckets;

	private RpcStrategyRendezvousHashing(@NotNull ToIntFunction<?> hashFunction, int minShards,
			ToLongBiFunction<?, Integer> hashBucketFunction, int buckets,
			Map<Object, RpcStrategy> shards) {
		this.hashFunction = hashFunction;
		this.minShards = minShards;
		this.hashBucketFunction = hashBucketFunction;
		this.buckets = buckets;
		this.shards = shards;
	}

	public static <T> RpcStrategyRendezvousHashing create(ToIntFunction<T> hashFunction) {
		return new RpcStrategyRendezvousHashing(hashFunction, MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT,
				DEFAULT_BUCKET_HASH_FUNCTION, DEFAULT_BUCKET_CAPACITY, new HashMap<>());
	}

	public RpcStrategyRendezvousHashing withMinActiveShards(int minShards) {
		checkArgument(minShards > 0, "minSubStrategiesForCreation must be greater than 0");
		return new RpcStrategyRendezvousHashing(hashFunction, minShards, hashBucketFunction, buckets, shards);
	}

	public RpcStrategyRendezvousHashing withHashBucketFunction(ToLongBiFunction<?, Integer> hashBucketFunction) {
		return new RpcStrategyRendezvousHashing(hashFunction, minShards, hashBucketFunction, buckets, shards);
	}

	public RpcStrategyRendezvousHashing withHashBuckets(int buckets) {
		checkArgument((buckets & (buckets - 1)) == 0, "Buckets number must be a power-of-two, got %d", buckets);
		return new RpcStrategyRendezvousHashing(hashFunction, minShards, hashBucketFunction, buckets, shards);
	}

	public RpcStrategyRendezvousHashing withShard(Object shardId, @NotNull RpcStrategy strategy) {
		shards.put(shardId, strategy);
		return this;
	}

	public RpcStrategyRendezvousHashing withShards(InetSocketAddress... addresses) {
		return withShards(Arrays.asList(addresses));
	}

	public RpcStrategyRendezvousHashing withShards(List<InetSocketAddress> addresses) {
		for (InetSocketAddress address : addresses) {
			shards.put(address, RpcStrategySingleServer.create(address));
		}
		return this;
	}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		HashSet<InetSocketAddress> result = new HashSet<>();
		for (RpcStrategy strategy : shards.values()) {
			result.addAll(strategy.getAddresses());
		}
		return result;
	}

	@Override
	public @Nullable RpcSender createSender(RpcClientConnectionPool pool) {
		Map<Object, RpcSender> shardsSenders = new HashMap<>();
		for (Map.Entry<Object, RpcStrategy> entry : shards.entrySet()) {
			Object shardId = entry.getKey();
			RpcStrategy strategy = entry.getValue();
			RpcSender sender = strategy.createSender(pool);
			if (sender != null) {
				shardsSenders.put(shardId, sender);
			}
		}
		if (shardsSenders.size() < minShards) {
			return null;
		}
		if (shardsSenders.size() == 1) {
			return first(shardsSenders.values());
		}

		RpcSender[] sendersBuckets = new RpcSender[buckets];
		for (int n = 0; n < sendersBuckets.length; n++) {
			RpcSender chosenSender = null;
			long max = Long.MIN_VALUE;
			for (Map.Entry<Object, RpcSender> entry : shardsSenders.entrySet()) {
				Object key = entry.getKey();
				RpcSender sender = entry.getValue();
				//noinspection unchecked
				long hash = ((ToLongBiFunction<Object, Integer>) hashBucketFunction).applyAsLong(key, n);
				if (hash >= max) {
					chosenSender = sender;
					max = hash;
				}
			}
			assert chosenSender != null;
			sendersBuckets[n] = chosenSender;
		}
		return new Sender(hashFunction, sendersBuckets);
	}

	static final class Sender implements RpcSender {
		private final ToIntFunction<Object> hashFunction;
		private final RpcSender[] hashBuckets;

		Sender(@NotNull ToIntFunction<?> hashFunction, RpcSender[] hashBuckets) {
			//noinspection unchecked
			this.hashFunction = (ToIntFunction<Object>) hashFunction;
			this.hashBuckets = hashBuckets;
		}

		@Override
		public <I, O> void sendRequest(I request, int timeout, @NotNull Callback<O> cb) {
			int hash = hashFunction.applyAsInt(request);
			RpcSender sender = hashBuckets[hash & (hashBuckets.length - 1)];
			sender.sendRequest(request, timeout, cb);
		}
	}

}
