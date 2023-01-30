package io.activej.crdt.storage.cluster;

import io.activej.common.builder.AbstractBuilder;
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.crdt.storage.cluster.IDiscoveryService.PartitionScheme;
import io.activej.rpc.client.sender.RpcStrategy;
import io.activej.rpc.client.sender.RpcStrategy_RendezvousHashing;
import io.activej.rpc.client.sender.RpcStrategy_Sharding;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.*;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import static io.activej.common.Utils.difference;
import static io.activej.crdt.storage.cluster.RendezvousHashSharder.NUMBER_OF_BUCKETS;
import static java.util.stream.Collectors.toSet;

public final class RendezvousPartitionScheme<P> implements PartitionScheme<P> {
	private final List<RendezvousPartitionGroup<P>> partitionGroups = new ArrayList<>();
	private ToIntFunction<?> keyHashFn = Object::hashCode;
	@SuppressWarnings("unchecked")
	private Function<P, Object> partitionIdGetter = (Function<P, Object>) Function.identity();
	private Function<P, RpcStrategy> rpcProvider;
	private Function<P, ICrdtStorage<?, ?>> crdtProvider;

	@SafeVarargs
	public static <P> RendezvousPartitionScheme<P> create(RendezvousPartitionGroup<P>... partitionGroups) {
		return builder(partitionGroups).build();
	}

	public static <P> RendezvousPartitionScheme<P> create(List<RendezvousPartitionGroup<P>> partitionGroups) {
		return builder(partitionGroups).build();
	}

	@SafeVarargs
	public static <P> RendezvousPartitionScheme<P>.Builder builder(RendezvousPartitionGroup<P>... partitionGroups) {
		return builder(List.of(partitionGroups));
	}

	public static <P> RendezvousPartitionScheme<P>.Builder builder(List<RendezvousPartitionGroup<P>> partitionGroups) {
		RendezvousPartitionScheme<P> scheme = new RendezvousPartitionScheme<>();
		scheme.partitionGroups.addAll(partitionGroups);
		return scheme.new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, RendezvousPartitionScheme<P>> {
		private Builder() {}

		public Builder withPartitionIdGetter(Function<P, Object> partitionIdGetter) {
			checkNotBuilt(this);
			RendezvousPartitionScheme.this.partitionIdGetter = partitionIdGetter;
			return this;
		}

		public Builder withCrdtProvider(Function<P, ICrdtStorage<?, ?>> crdtProvider) {
			checkNotBuilt(this);
			RendezvousPartitionScheme.this.crdtProvider = crdtProvider;
			return this;
		}

		public Builder withRpcProvider(Function<P, RpcStrategy> rpcProvider) {
			checkNotBuilt(this);
			RendezvousPartitionScheme.this.rpcProvider = rpcProvider;
			return this;
		}

		public Builder withPartitionGroup(RendezvousPartitionGroup<P> partitionGroup) {
			checkNotBuilt(this);
			RendezvousPartitionScheme.this.partitionGroups.add(partitionGroup);
			return this;
		}

		public <K extends Comparable<K>> Builder withKeyHashFn(ToIntFunction<K> keyHashFn) {
			checkNotBuilt(this);
			RendezvousPartitionScheme.this.keyHashFn = keyHashFn;
			return this;
		}

		@Override
		protected RendezvousPartitionScheme<P> doBuild() {
			return RendezvousPartitionScheme.this;
		}
	}

	@Override
	public Set<P> getPartitions() {
		return partitionGroups.stream().flatMap(g -> g.getPartitionIds().stream()).collect(toSet());
	}

	@Override
	public ICrdtStorage<?, ?> provideCrdtConnection(P partition) {
		return crdtProvider.apply(partition);
	}

	@Override
	public RpcStrategy provideRpcConnection(P partition) {
		return rpcProvider.apply(partition);
	}

	@Override
	public <K extends Comparable<K>> @Nullable Sharder<K> createSharder(List<P> alive) {
		Set<P> aliveSet = new HashSet<>(alive);
		List<RendezvousHashSharder<K>> sharders = new ArrayList<>();
		for (RendezvousPartitionGroup<P> partitionGroup : partitionGroups) {
			int deadPartitions = difference(partitionGroup.getPartitionIds(), aliveSet).size();

			if (partitionGroup.isRepartition()) {
				int aliveSize = partitionGroup.getPartitionIds().size() - deadPartitions;
				if (aliveSize < partitionGroup.getReplicaCount()) return null;
			} else if (deadPartitions != 0) return null;

			//noinspection unchecked
			RendezvousHashSharder<K> sharder = RendezvousHashSharder.create(
					((ToIntFunction<K>) keyHashFn),
					p -> partitionIdGetter.apply(p).hashCode(),
					partitionGroup.getPartitionIds(),
					alive,
					partitionGroup.getReplicaCount(), partitionGroup.isRepartition());
			sharders.add(sharder);
		}
		return RendezvousHashSharder.unionOf(sharders);
	}

	@Override
	public <K extends Comparable<K>> RpcStrategy createRpcStrategy(Function<Object, K> keyGetter) {
		List<RpcStrategy> rendezvousHashings = new ArrayList<>();
		for (RendezvousPartitionGroup<P> partitionGroup : partitionGroups) {
			if (!partitionGroup.isActive()) continue;
			//noinspection unchecked
			rendezvousHashings.add(
					RpcStrategy_RendezvousHashing.builder(req ->
									((ToIntFunction<K>) keyHashFn).applyAsInt(keyGetter.apply(req)))
							.withBuckets(NUMBER_OF_BUCKETS)
							.withHashBucketFn((p, bucket) -> RendezvousHashSharder.hashBucket(partitionIdGetter.apply((P) p).hashCode(), bucket))
							.initialize(rendezvousHashing -> {
								for (P partitionId : partitionGroup.getPartitionIds()) {
									rendezvousHashing.withShard(partitionId, provideRpcConnection(partitionId));
								}
								if (!partitionGroup.isRepartition()) {
									rendezvousHashing.withReshardings(partitionGroup.getReplicaCount());
								}
							})
							.build());
		}
		final int count = rendezvousHashings.size();
		return RpcStrategy_Sharding.create(item -> keyGetter.apply(item).hashCode() % count, rendezvousHashings);
	}

	@Override
	public boolean isReadValid(Collection<P> alive) {
		Set<P> aliveSet = new HashSet<>(alive);
		for (RendezvousPartitionGroup<P> partitionGroup : partitionGroups) {
			int deadPartitions = difference(partitionGroup.getPartitionIds(), aliveSet).size();
			if (deadPartitions < partitionGroup.getReplicaCount()) {
				return true;
			}
		}
		return false;
	}

	@VisibleForTesting
	List<RendezvousPartitionGroup<P>> getPartitionGroups() {
		return partitionGroups;
	}
}
