package io.activej.crdt.storage.cluster;

import io.activej.crdt.storage.CrdtStorage;
import io.activej.rpc.client.sender.RpcStrategies;
import io.activej.rpc.client.sender.RpcStrategy;
import io.activej.rpc.client.sender.RpcStrategyRendezvousHashing;
import io.activej.rpc.hash.ShardingFunction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import static io.activej.crdt.storage.cluster.RendezvousHashSharder.NUMBER_OF_BUCKETS;

public final class RendezvousPartitionings<K extends Comparable<K>, S, P> implements DiscoveryService.Partitionings<K, S, P> {
	private final Map<P, CrdtStorage<K, S>> partitions;
	private final List<Partitioning<P>> partitionings;
	private final RendezvousHashSharder.HashBucketFunction<P> hashBucketFn;
	private final ToIntFunction<K> hashFn;

	@SuppressWarnings("unchecked")
	private RendezvousPartitionings(Map<P, ? extends CrdtStorage<K, S>> partitions, List<? extends Partitioning<P>> partitionings, RendezvousHashSharder.HashBucketFunction<P> hashBucketFn, ToIntFunction<K> hashFn) {
		this.partitions = (Map<P, CrdtStorage<K, S>>) partitions;
		this.partitionings = (List<Partitioning<P>>) partitionings;
		this.hashBucketFn = hashBucketFn;
		this.hashFn = hashFn;
	}

	@SafeVarargs
	public static <K extends Comparable<K>, S, P> RendezvousPartitionings<K, S, P> create(Map<P, ? extends CrdtStorage<K, S>> partitions,
			Partitioning<P>... partitionings) {
		return create(partitions, Arrays.asList(partitionings));
	}

	public static <K extends Comparable<K>, S, P> RendezvousPartitionings<K, S, P> create(Map<P, ? extends CrdtStorage<K, S>> partitions,
			List<Partitioning<P>> partitionings) {
		return new RendezvousPartitionings<>(partitions, partitionings, RendezvousHashSharder.HashBucketFunction.create(), Objects::hashCode);
	}

	public RendezvousPartitionings<K, S, P> withPartitioning(Partitioning<P> partitioning) {
		List<Partitioning<P>> partitionings = new ArrayList<>(this.partitionings);
		partitionings.add(partitioning);
		return new RendezvousPartitionings<>(partitions, partitionings, hashBucketFn, hashFn);
	}

	public RendezvousPartitionings<K, S, P> withHashBucketFn(RendezvousHashSharder.HashBucketFunction<P> hashBucketFn) {
		return new RendezvousPartitionings<>(partitions, partitionings, hashBucketFn, hashFn);
	}

	public RendezvousPartitionings<K, S, P> withHashFn(ToIntFunction<K> hashFn) {
		return new RendezvousPartitionings<>(partitions, partitionings, hashBucketFn, hashFn);
	}

	@Override
	public Map<P, CrdtStorage<K, S>> getPartitions() {
		return partitions;
	}

	@Override
	public @Nullable Sharder<K> createSharder(List<P> alive) {
		List<RendezvousHashSharder<K, P>> sharders = new ArrayList<>();
		for (Partitioning<P> partitioning : partitionings) {
			RendezvousHashSharder<K, P> sharder = RendezvousHashSharder.create(hashBucketFn, hashFn,
					partitioning.getSet(), alive, partitioning.getReplicas(), partitioning.isRepartition());
			sharders.add(sharder);
		}
		return Sharder.unionOf(sharders);
	}

	@Override
	public RpcStrategy createRpcStrategy(
			Function<P, @NotNull RpcStrategy> rpcStrategyResolver, Function<Object, K> keyGetter) {

		List<RpcStrategy> rendezvousHashings = new ArrayList<>();
		for (Partitioning<P> partitioning : partitionings) {
			if (!partitioning.isActive()) continue;
			RpcStrategyRendezvousHashing rendezvousHashing = RpcStrategyRendezvousHashing.create(req ->
							hashFn.applyAsInt(keyGetter.apply(req)))
					.withHashBuckets(NUMBER_OF_BUCKETS)
					.withHashBucketFunction((shardId, bucket) -> hashBucketFn.hash((P) shardId, bucket));
			for (P pid : partitioning.set) {
				rendezvousHashing.withShard(pid, rpcStrategyResolver.apply(pid));
			}
			rendezvousHashings.add(rendezvousHashing);
		}

		return RpcStrategies.sharding(
				new ShardingFunction<Object>() {
					final int count = rendezvousHashings.size();

					@Override
					public int getShard(Object item) {
						return keyGetter.apply(item).hashCode() % count;
					}
				},
				rendezvousHashings);
	}

	public static final class Partitioning<P> {
		private final Set<P> set;
		private final int replicas;
		private final boolean repartition;
		private final boolean active;

		private Partitioning(Set<P> set, int replicas, boolean repartition, boolean active) {
			this.set = set;
			this.replicas = replicas;
			this.repartition = repartition;
			this.active = active;
		}

		public static <P> Partitioning<P> create(Set<P> set, int replicas, boolean repartition, boolean active) {
			return new Partitioning<>(set, replicas, repartition, active);
		}

		public static <P> Partitioning<P> create(Set<P> set) {
			return new Partitioning<>(set, 1, false, true);
		}

		public Partitioning<P> withReplicas(int replicas) {
			return new Partitioning<>(set, replicas, repartition, active);
		}

		public Partitioning<P> withRepartition(boolean repartition) {
			return new Partitioning<>(set, replicas, repartition, active);
		}

		public Partitioning<P> withActive(boolean active) {
			return new Partitioning<>(set, replicas, repartition, active);
		}

		public Set<P> getSet() {
			return set;
		}

		public int getReplicas() {
			return replicas;
		}

		public boolean isRepartition() {
			return repartition;
		}

		public boolean isActive() {
			return active;
		}
	}

}
