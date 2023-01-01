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
import io.activej.common.initializer.WithInitializer;
import io.activej.rpc.client.RpcClientConnectionPool;
import io.activej.rpc.protocol.RpcException;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.ToIntFunction;
import java.util.function.ToLongBiFunction;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;

/**
 * Strategies used in RPC can be divided in three following categories:
 * <ul>
 *     <li>Getting a single RPC-service</li>
 *     <li>Failover</li>
 *     <li>Load balancing</li>
 *     <li>Rendezvous hashing</li>
 * </ul>
 */
public final class RpcStrategies {
	static final RpcException NO_SENDER_AVAILABLE_EXCEPTION = new RpcException("No senders available");

	public static SingleServer server(InetSocketAddress address) {
		return SingleServer.create(address);
	}

	public static List<RpcStrategy> servers(InetSocketAddress... addresses) {
		return servers(List.of(addresses));
	}

	public static List<RpcStrategy> servers(List<InetSocketAddress> addresses) {
		checkArgument(!addresses.isEmpty(), "At least one address must be present");
		return addresses.stream()
				.map(SingleServer::create)
				.collect(toList());
	}

	public static final class FirstAvailable implements RpcStrategy {
		private final List<RpcStrategy> list;

		private FirstAvailable(List<RpcStrategy> list) {
			this.list = list;
		}

		public static FirstAvailable create(RpcStrategy... list) {return create(List.of(list));}

		public static FirstAvailable create(List<RpcStrategy> list) {return new FirstAvailable(list);}

		@Override
		public Set<InetSocketAddress> getAddresses() {
			return Utils.getAddresses(list);
		}

		@Override
		public @Nullable RpcSender createSender(RpcClientConnectionPool pool) {
			List<RpcSender> senders = Utils.listOfSenders(list, pool);
			if (senders.isEmpty())
				return null;
			return senders.get(0);
		}
	}

	public static final class FirstValidResult implements RpcStrategy {
		@FunctionalInterface
		public interface ResultValidator<T> {
			boolean isValidResult(T value);
		}

		private static final ResultValidator<?> DEFAULT_RESULT_VALIDATOR = new DefaultResultValidator<>();

		private final List<RpcStrategy> list;

		private final ResultValidator<?> resultValidator;
		private final @Nullable Exception noValidResultException;

		private FirstValidResult(List<RpcStrategy> list, ResultValidator<?> resultValidator,
				@Nullable Exception noValidResultException) {
			this.list = list;
			this.resultValidator = resultValidator;
			this.noValidResultException = noValidResultException;
		}

		public static FirstValidResult create(RpcStrategy... list) {
			return create(List.of(list));
		}

		public static FirstValidResult create(List<RpcStrategy> list) {
			return new FirstValidResult(list, DEFAULT_RESULT_VALIDATOR, null);
		}

		public FirstValidResult withResultValidator(ResultValidator<?> resultValidator) {
			return new FirstValidResult(list, resultValidator, noValidResultException);
		}

		public FirstValidResult withNoValidResultException(Exception e) {
			return new FirstValidResult(list, resultValidator, e);
		}

		@Override
		public Set<InetSocketAddress> getAddresses() {
			return Utils.getAddresses(list);
		}

		@Override
		public @Nullable RpcSender createSender(RpcClientConnectionPool pool) {
			List<RpcSender> senders = Utils.listOfSenders(list, pool);
			if (senders.isEmpty())
				return null;
			return new Sender(senders, resultValidator, noValidResultException);
		}

		static final class Sender implements RpcSender {
			private final RpcSender[] subSenders;
			private final ResultValidator<?> resultValidator;
			private final @Nullable Exception noValidResultException;

			Sender(List<RpcSender> senders, ResultValidator<?> resultValidator,
					@Nullable Exception noValidResultException) {
				assert !senders.isEmpty();
				this.subSenders = senders.toArray(new RpcSender[0]);
				this.resultValidator = resultValidator;
				this.noValidResultException = noValidResultException;
			}

			@SuppressWarnings("unchecked")
			@Override
			public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
				FirstResultCallback<O> firstResultCallback = new FirstResultCallback<>(subSenders.length, (FirstValidResult.ResultValidator<O>) resultValidator, cb, noValidResultException);
				for (RpcSender sender : subSenders) {
					sender.sendRequest(request, timeout, firstResultCallback);
				}
			}
		}

		static final class FirstResultCallback<T> implements Callback<T> {
			private int expectedCalls;
			private final ResultValidator<T> resultValidator;
			private final Callback<T> cb;
			private Exception lastException;
			private final @Nullable Exception noValidResultException;

			FirstResultCallback(int expectedCalls, ResultValidator<T> resultValidator, Callback<T> cb,
					@Nullable Exception noValidResultException) {
				assert expectedCalls > 0;
				this.expectedCalls = expectedCalls;
				this.cb = cb;
				this.resultValidator = resultValidator;
				this.noValidResultException = noValidResultException;
			}

			@Override
			public void accept(T result, @Nullable Exception e) {
				if (e == null) {
					if (--expectedCalls >= 0) {
						if (resultValidator.isValidResult(result)) {
							expectedCalls = 0;
							cb.accept(result, null);
						} else {
							if (expectedCalls == 0) {
								cb.accept(null, lastException != null ? lastException : noValidResultException);
							}
						}
					}
				} else {
					lastException = e; // last Exception
					if (--expectedCalls == 0) {
						cb.accept(null, lastException);
					}
				}
			}
		}

		private static final class DefaultResultValidator<T> implements ResultValidator<T> {
			@Override
			public boolean isValidResult(T input) {
				return input != null;
			}
		}

	}

	public static class RandomSampling implements RpcStrategy {
		private final Random random = new Random();
		private final Map<RpcStrategy, Integer> strategyToWeight = new HashMap<>();

		private RandomSampling() {}

		public static RandomSampling create() {return new RandomSampling();}

		public RandomSampling add(int weight, RpcStrategy strategy) {
			checkArgument(weight >= 0, "weight cannot be negative");
			checkArgument(!strategyToWeight.containsKey(strategy), "withStrategy is already added");

			strategyToWeight.put(strategy, weight);

			return this;
		}

		@Override
		public Set<InetSocketAddress> getAddresses() {
			HashSet<InetSocketAddress> result = new HashSet<>();
			for (RpcStrategy strategy : strategyToWeight.keySet()) {
				result.addAll(strategy.getAddresses());
			}
			return result;
		}

		@Override
		public RpcSender createSender(RpcClientConnectionPool pool) {
			Map<RpcSender, Integer> senderToWeight = new HashMap<>();
			int totalWeight = 0;
			for (Map.Entry<RpcStrategy, Integer> entry : strategyToWeight.entrySet()) {
				RpcSender sender = entry.getKey().createSender(pool);
				if (sender != null) {
					int weight = entry.getValue();
					senderToWeight.put(sender, weight);
					totalWeight += weight;
				}
			}

			if (totalWeight == 0) {
				return null;
			}

			long randomLong = random.nextLong();
			long seed = randomLong != 0L ? randomLong : 2347230858016798896L;

			return new RandomSamplingSender(senderToWeight, seed);
		}

		private static final class RandomSamplingSender implements RpcSender {
			private final List<RpcSender> senders;
			private final int[] cumulativeWeights;
			private final int totalWeight;

			private long lastRandomLong;

			RandomSamplingSender(Map<RpcSender, Integer> senderToWeight, long seed) {
				assert !senderToWeight.containsKey(null);

				senders = new ArrayList<>(senderToWeight.size());
				cumulativeWeights = new int[senderToWeight.size()];
				int currentCumulativeWeight = 0;
				int currentSender = 0;
				for (Map.Entry<RpcSender, Integer> entry : senderToWeight.entrySet()) {
					currentCumulativeWeight += entry.getValue();
					senders.add(entry.getKey());
					cumulativeWeights[currentSender++] = currentCumulativeWeight;
				}
				totalWeight = currentCumulativeWeight;

				lastRandomLong = seed;
			}

			@Override
			public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
				lastRandomLong ^= (lastRandomLong << 21);
				lastRandomLong ^= (lastRandomLong >>> 35);
				lastRandomLong ^= (lastRandomLong << 4);
				int currentRandomValue = (int) ((lastRandomLong & Long.MAX_VALUE) % totalWeight);
				int lowerIndex = 0;
				int upperIndex = cumulativeWeights.length;
				while (lowerIndex != upperIndex) {
					int middle = (lowerIndex + upperIndex) / 2;
					if (currentRandomValue >= cumulativeWeights[middle]) {
						lowerIndex = middle + 1;
					} else {
						upperIndex = middle;
					}
				}
				senders.get(lowerIndex).sendRequest(request, timeout, cb);
			}
		}

	}

	public static final class RendezvousHashing implements RpcStrategy, WithInitializer<RendezvousHashing> {
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

		private RendezvousHashing(ToIntFunction<?> hashFn) {
			this.hashFn = hashFn;
		}

		public static <T> RendezvousHashing create(ToIntFunction<T> hashFunction) {
			return new RendezvousHashing(hashFunction);
		}

		public RendezvousHashing withHashBucketFn(ToLongBiFunction<Object, Integer> hashBucketFn) {
			this.hashBucketFn = hashBucketFn;
			return this;
		}

		public RendezvousHashing withBuckets(int buckets) {
			checkArgument((buckets & (buckets - 1)) == 0);
			this.buckets = buckets;
			return this;
		}

		public RendezvousHashing withMinActiveShards(int minActiveShards) {
			this.minActiveShards = minActiveShards;
			return this;
		}

		public RendezvousHashing withReshardings(int reshardings) {
			this.reshardings = reshardings;
			return this;
		}

		public RendezvousHashing withShard(Object shardId, RpcStrategy strategy) {
			shards.put(shardId, strategy);
			return this;
		}

		public RendezvousHashing withShards(InetSocketAddress... addresses) {
			return withShards(List.of(addresses));
		}

		public RendezvousHashing withShards(List<InetSocketAddress> addresses) {
			for (InetSocketAddress address : addresses) {
				shards.put(address, SingleServer.create(address));
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

	public static final class RoundRobin implements RpcStrategy {
		private final List<RpcStrategy> list;
		private final int minActiveSubStrategies;

		private RoundRobin(List<RpcStrategy> list, int minActiveSubStrategies) {
			this.list = list;
			this.minActiveSubStrategies = minActiveSubStrategies;
		}

		public static RoundRobin create(RpcStrategy... list) {
			return create(List.of(list));
		}

		public static RoundRobin create(List<RpcStrategy> list) {
			return new RoundRobin(list, 0);
		}

		public RoundRobin withMinActiveSubStrategies(int minActiveSubStrategies) {
			return new RoundRobin(list, minActiveSubStrategies);
		}

		@Override
		public Set<InetSocketAddress> getAddresses() {
			return Utils.getAddresses(list);
		}

		@Override
		public @Nullable RpcSender createSender(RpcClientConnectionPool pool) {
			List<RpcSender> subSenders = Utils.listOfSenders(list, pool);
			if (subSenders.size() < minActiveSubStrategies)
				return null;
			if (subSenders.isEmpty())
				return null;
			if (subSenders.size() == 1)
				return subSenders.get(0);
			return new Sender(subSenders);
		}

		private static final class Sender implements RpcSender {
			private int nextSender;
			private final RpcSender[] subSenders;

			Sender(List<RpcSender> senders) {
				assert !senders.isEmpty();
				this.subSenders = senders.toArray(new RpcSender[0]);
				this.nextSender = 0;
			}

			@Override
			public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
				RpcSender sender = subSenders[nextSender];
				nextSender = (nextSender + 1) % subSenders.length;
				sender.sendRequest(request, timeout, cb);
			}

		}
	}

	public static final class Sharding implements RpcStrategy {
		private final List<RpcStrategy> list;
		private final ToIntFunction<?> shardingFunction;
		private final int minActiveSubStrategies;

		private Sharding(ToIntFunction<?> shardingFunction, List<RpcStrategy> list, int minActiveSubStrategies) {
			this.shardingFunction = shardingFunction;
			this.list = list;
			this.minActiveSubStrategies = minActiveSubStrategies;
		}

		public static <T> Sharding create(ToIntFunction<T> shardingFunction, List<RpcStrategy> list) {
			return new Sharding(shardingFunction, list, 0);
		}

		public static <T> Sharding create(ToIntFunction<T> shardingFunction, RpcStrategy... list) {
			return new Sharding(shardingFunction, List.of(list), 0);
		}

		public Sharding withMinActiveSubStrategies(int minActiveSubStrategies) {
			return new Sharding(shardingFunction, list, minActiveSubStrategies);
		}

		@Override
		public Set<InetSocketAddress> getAddresses() {
			return Utils.getAddresses(list);
		}

		@Override
		public @Nullable RpcSender createSender(RpcClientConnectionPool pool) {
			List<RpcSender> subSenders = Utils.listOfNullableSenders(list, pool);
			int activeSenders = 0;
			for (RpcSender subSender : subSenders) {
				if (subSender != null) {
					activeSenders++;
				}
			}
			if (activeSenders < minActiveSubStrategies)
				return null;
			if (subSenders.isEmpty())
				return null;
			if (subSenders.size() == 1)
				return subSenders.get(0);
			return new Sender(shardingFunction, subSenders);
		}

		private static final class Sender implements RpcSender {
			private final ToIntFunction<Object> shardingFunction;
			private final RpcSender[] subSenders;

			Sender(ToIntFunction<?> shardingFunction, List<RpcSender> senders) {
				assert !senders.isEmpty();
				//noinspection unchecked
				this.shardingFunction = (ToIntFunction<Object>) shardingFunction;
				this.subSenders = senders.toArray(new RpcSender[0]);
			}

			@Override
			public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
				int shardIndex = shardingFunction.applyAsInt(request);
				RpcSender sender = subSenders[shardIndex];
				if (sender != null) {
					sender.sendRequest(request, timeout, cb);
				} else {
					cb.accept(null, NO_SENDER_AVAILABLE_EXCEPTION);
				}
			}

		}
	}

	public static final class SingleServer implements RpcStrategy {

		private final InetSocketAddress address;

		private SingleServer(InetSocketAddress address) {
			this.address = address;
		}

		public static SingleServer create(InetSocketAddress address) {
			return new SingleServer(address);
		}

		@Override
		public Set<InetSocketAddress> getAddresses() {
			return Set.of(address);
		}

		@Override
		public RpcSender createSender(RpcClientConnectionPool pool) {
			return pool.get(address);
		}

		public InetSocketAddress getAddress() {
			return address;
		}
	}

	public static final class TypeDispatching implements RpcStrategy {
		private final Map<Class<?>, RpcStrategy> dataTypeToStrategy = new HashMap<>();
		private RpcStrategy defaultStrategy;

		private TypeDispatching() {}

		public static TypeDispatching create() {return new TypeDispatching();}

		public TypeDispatching on(Class<?> dataType, RpcStrategy strategy) {
			checkState(!dataTypeToStrategy.containsKey(dataType),
					() -> "Strategy for type " + dataType.toString() + " is already set");
			dataTypeToStrategy.put(dataType, strategy);
			return this;
		}

		public TypeDispatching onDefault(RpcStrategy strategy) {
			checkState(defaultStrategy == null, "Default Strategy is already set");
			defaultStrategy = strategy;
			return this;
		}

		@Override
		public Set<InetSocketAddress> getAddresses() {
			HashSet<InetSocketAddress> result = new HashSet<>();
			for (RpcStrategy strategy : dataTypeToStrategy.values()) {
				result.addAll(strategy.getAddresses());
			}
			result.addAll(defaultStrategy.getAddresses());
			return result;
		}

		@Override
		public @Nullable RpcSender createSender(RpcClientConnectionPool pool) {
			HashMap<Class<?>, RpcSender> typeToSender = new HashMap<>();
			for (Map.Entry<Class<?>, RpcStrategy> entry : dataTypeToStrategy.entrySet()) {
				RpcSender sender = entry.getValue().createSender(pool);
				if (sender == null)
					return null;
				typeToSender.put(entry.getKey(), sender);
			}
			RpcSender defaultSender = null;
			if (defaultStrategy != null) {
				defaultSender = defaultStrategy.createSender(pool);
				if (typeToSender.isEmpty())
					return defaultSender;
				if (defaultSender == null)
					return null;
			}
			return new Sender(typeToSender, defaultSender);
		}

		static final class Sender implements RpcSender {
			private final HashMap<Class<?>, RpcSender> typeToSender;
			private final @Nullable RpcSender defaultSender;

			Sender(HashMap<Class<?>, RpcSender> typeToSender, @Nullable RpcSender defaultSender) {
				this.typeToSender = typeToSender;
				this.defaultSender = defaultSender;
			}

			@Override
			public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
				RpcSender sender = typeToSender.get(request.getClass());
				if (sender == null) {
					sender = defaultSender;
				}
				if (sender != null) {
					sender.sendRequest(request, timeout, cb);
				} else {
					cb.accept(null, NO_SENDER_AVAILABLE_EXCEPTION);
				}
			}
		}
	}
}

