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
import io.activej.common.builder.AbstractBuilder;
import io.activej.rpc.client.RpcClientConnectionPool;
import io.activej.rpc.protocol.RpcException;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.ToIntFunction;
import java.util.function.ToLongBiFunction;

import static io.activej.common.Checks.checkArgument;
import static io.activej.rpc.client.sender.RpcStrategies.server;
import static java.lang.Math.min;

public final class RpcStrategy_RendezvousHashing implements RpcStrategy {
	private static final int DEFAULT_BUCKET_CAPACITY = 2048;
	private static final ToLongBiFunction<Object, Integer> DEFAULT_HASH_BUCKET_FN = (shardId, bucketN) ->
			(int) HashUtils.murmur3hash(((long) shardId.hashCode() << 32) | (bucketN & 0xFFFFFFFFL));
	private static final int DEFAULT_MIN_ACTIVE_SHARDS = 1;
	private static final int DEFAULT_MAX_RESHARDINGS = Integer.MAX_VALUE;

	private final Map<Object, RpcStrategy> shards = new HashMap<>();
	private final ToIntFunction<?> hashFn;
	private ToLongBiFunction<Object, Integer> hashBucketFn = DEFAULT_HASH_BUCKET_FN;
	private int buckets = DEFAULT_BUCKET_CAPACITY;
	private int minActiveShards = DEFAULT_MIN_ACTIVE_SHARDS;
	private int reshardings = DEFAULT_MAX_RESHARDINGS;

	private RpcStrategy_RendezvousHashing(ToIntFunction<?> hashFn) {
		this.hashFn = hashFn;
	}

	public static <T> Builder builder(ToIntFunction<T> hashFn) {
		return new RpcStrategy_RendezvousHashing(hashFn).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, RpcStrategy_RendezvousHashing> {
		private Builder() {}

		public Builder withHashBucketFn(ToLongBiFunction<Object, Integer> hashBucketFn) {
			checkNotBuilt(this);
			RpcStrategy_RendezvousHashing.this.hashBucketFn = hashBucketFn;
			return this;
		}

		public Builder withBuckets(int buckets) {
			checkNotBuilt(this);
			checkArgument((buckets & (buckets - 1)) == 0);
			RpcStrategy_RendezvousHashing.this.buckets = buckets;
			return this;
		}

		public Builder withMinActiveShards(int minActiveShards) {
			checkNotBuilt(this);
			RpcStrategy_RendezvousHashing.this.minActiveShards = minActiveShards;
			return this;
		}

		public Builder withReshardings(int reshardings) {
			checkNotBuilt(this);
			RpcStrategy_RendezvousHashing.this.reshardings = reshardings;
			return this;
		}

		public Builder withShard(Object shardId, RpcStrategy strategy) {
			checkNotBuilt(this);
			shards.put(shardId, strategy);
			return this;
		}

		public Builder withShards(InetSocketAddress... addresses) {
			return withShards(List.of(addresses));
		}

		public Builder withShards(List<InetSocketAddress> addresses) {
			checkNotBuilt(this);
			for (InetSocketAddress address : addresses) {
				shards.put(address, server(address));
			}
			return this;
		}

		@Override
		protected RpcStrategy_RendezvousHashing doBuild() {
			return RpcStrategy_RendezvousHashing.this;
		}
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
		int activeShards = 0;
		for (Map.Entry<Object, RpcStrategy> entry : shards.entrySet()) {
			Object shardId = entry.getKey();
			RpcStrategy strategy = entry.getValue();
			RpcSender sender = strategy.createSender(pool);
			if (sender != null) {
				activeShards++;
			}
			shardsSenders.put(shardId, sender);
		}

		if (activeShards < minActiveShards) {
			return null;
		}

		RpcSender[] sendersBuckets = new RpcSender[buckets];

		//noinspection NullableProblems
		class ShardIdAndSender {
			final Object shardId;
			final @Nullable RpcSender rpcSender;
			private long hash;

			public long getHash() {
				return hash;
			}

			ShardIdAndSender(Object shardId, RpcSender rpcSender) {
				this.shardId = shardId;
				this.rpcSender = rpcSender;
			}
		}

		ShardIdAndSender[] toSort = new ShardIdAndSender[shards.size()];
		int i = 0;
		for (Map.Entry<Object, RpcSender> entry : shardsSenders.entrySet()) {
			toSort[i++] = new ShardIdAndSender(entry.getKey(), entry.getValue());
		}

		for (int bucket = 0; bucket < sendersBuckets.length; bucket++) {
			for (ShardIdAndSender obj : toSort) {
				obj.hash = hashBucketFn.applyAsLong(obj.shardId, bucket);
			}

			Arrays.sort(toSort, Comparator.comparingLong(ShardIdAndSender::getHash).reversed());

			for (int j = 0; j < min(shards.size(), reshardings); j++) {
				RpcSender rpcSender = toSort[j].rpcSender;
				if (rpcSender != null) {
					sendersBuckets[bucket] = rpcSender;
					break;
				}
			}
		}

		return new Sender(hashFn, sendersBuckets);
	}

	static final class Sender implements RpcSender {
		private final ToIntFunction<Object> hashFunction;
		private final @Nullable RpcSender[] hashBuckets;

		Sender(ToIntFunction<?> hashFunction, @Nullable RpcSender[] hashBuckets) {
			//noinspection unchecked
			this.hashFunction = (ToIntFunction<Object>) hashFunction;
			this.hashBuckets = hashBuckets;
		}

		@Override
		public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
			int hash = hashFunction.applyAsInt(request);
			RpcSender sender = hashBuckets[hash & (hashBuckets.length - 1)];

			if (sender != null) {
				sender.sendRequest(request, timeout, cb);
			} else {
				cb.accept(null, new RpcException("No sender for request: " + request));
			}
		}
	}

}
