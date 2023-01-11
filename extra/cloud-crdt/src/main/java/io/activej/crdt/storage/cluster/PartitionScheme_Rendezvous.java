package io.activej.crdt.storage.cluster;

import io.activej.common.initializer.WithInitializer;
import io.activej.crdt.storage.AsyncCrdtStorage;
import io.activej.crdt.storage.cluster.AsyncDiscoveryService.PartitionScheme;
import io.activej.rpc.client.sender.RpcStrategy;
import io.activej.rpc.client.sender.RpcStrategy_RendezvousHashing;
import io.activej.rpc.client.sender.RpcStrategy_Sharding;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.*;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import static io.activej.common.Utils.difference;
import static io.activej.crdt.storage.cluster.Sharder_RendezvousHash.NUMBER_OF_BUCKETS;
import static java.util.stream.Collectors.toSet;

public final class PartitionScheme_Rendezvous<P> implements PartitionScheme<P>, WithInitializer<PartitionScheme_Rendezvous<P>> {
	private final List<RendezvousPartitionGroup<P>> partitionGroups = new ArrayList<>();
	private ToIntFunction<?> keyHashFn = Object::hashCode;
	@SuppressWarnings("unchecked")
	private Function<P, Object> partitionIdGetter = (Function<P, Object>) Function.identity();
	private Function<P, RpcStrategy> rpcProvider;
	private Function<P, AsyncCrdtStorage<?, ?>> crdtProvider;

	@SafeVarargs
	public static <P> PartitionScheme_Rendezvous<P> create(RendezvousPartitionGroup<P>... partitionGroups) {
		return create(List.of(partitionGroups));
	}

	public static <P> PartitionScheme_Rendezvous<P> create(List<RendezvousPartitionGroup<P>> partitionGroups) {
		PartitionScheme_Rendezvous<P> scheme = new PartitionScheme_Rendezvous<>();
		scheme.partitionGroups.addAll(partitionGroups);
		return scheme;
	}

	public PartitionScheme_Rendezvous<P> withPartitionIdGetter(Function<P, Object> partitionIdGetter) {
		this.partitionIdGetter = partitionIdGetter;
		return this;
	}

	public PartitionScheme_Rendezvous<P> withCrdtProvider(Function<P, AsyncCrdtStorage<?, ?>> crdtProvider) {
		this.crdtProvider = crdtProvider;
		return this;
	}

	public PartitionScheme_Rendezvous<P> withRpcProvider(Function<P, RpcStrategy> rpcProvider) {
		this.rpcProvider = rpcProvider;
		return this;
	}

	public PartitionScheme_Rendezvous<P> withPartitionGroup(RendezvousPartitionGroup<P> partitionGroup) {
		this.partitionGroups.add(partitionGroup);
		return this;
	}

	public <K extends Comparable<K>> PartitionScheme_Rendezvous<P> withKeyHashFn(ToIntFunction<K> keyHashFn) {
		this.keyHashFn = keyHashFn;
		return this;
	}

	@Override
	public Set<P> getPartitions() {
		return partitionGroups.stream().flatMap(g -> g.getPartitionIds().stream()).collect(toSet());
	}

	@Override
	public AsyncCrdtStorage<?, ?> provideCrdtConnection(P partition) {
		return crdtProvider.apply(partition);
	}

	@Override
	public RpcStrategy provideRpcConnection(P partition) {
		return rpcProvider.apply(partition);
	}

	@Override
	public <K extends Comparable<K>> @Nullable Sharder<K> createSharder(List<P> alive) {
		Set<P> aliveSet = new HashSet<>(alive);
		List<Sharder_RendezvousHash<K>> sharders = new ArrayList<>();
		for (RendezvousPartitionGroup<P> partitionGroup : partitionGroups) {
			int deadPartitions = difference(partitionGroup.getPartitionIds(), aliveSet).size();

			if (partitionGroup.isRepartition()) {
				int aliveSize = partitionGroup.getPartitionIds().size() - deadPartitions;
				if (aliveSize < partitionGroup.getReplicaCount()) return null;
			} else if (deadPartitions != 0) return null;

			//noinspection unchecked
			Sharder_RendezvousHash<K> sharder = Sharder_RendezvousHash.create(
					((ToIntFunction<K>) keyHashFn),
					p -> partitionIdGetter.apply(p).hashCode(),
					partitionGroup.getPartitionIds(),
					alive,
					partitionGroup.getReplicaCount(), partitionGroup.isRepartition());
			sharders.add(sharder);
		}
		return Sharder_RendezvousHash.unionOf(sharders);
	}

	@Override
	public <K extends Comparable<K>> RpcStrategy createRpcStrategy(Function<Object, K> keyGetter) {

		List<RpcStrategy> rendezvousHashings = new ArrayList<>();
		for (RendezvousPartitionGroup<P> partitionGroup : partitionGroups) {
			if (!partitionGroup.isActive()) continue;
			//noinspection unchecked
			rendezvousHashings.add(
					RpcStrategy_RendezvousHashing.create(req ->
									((ToIntFunction<K>) keyHashFn).applyAsInt(keyGetter.apply(req)))
							.withBuckets(NUMBER_OF_BUCKETS)
							.withHashBucketFn((p, bucket) -> Sharder_RendezvousHash.hashBucket(partitionIdGetter.apply((P) p).hashCode(), bucket))
							.withInitializer(rendezvousHashing -> {
								for (P partitionId : partitionGroup.getPartitionIds()) {
									rendezvousHashing.withShard(partitionId, provideRpcConnection(partitionId));
								}
								if (!partitionGroup.isRepartition()) {
									rendezvousHashing.withReshardings(partitionGroup.getReplicaCount());
								}
							}));
		}

		return RpcStrategy_Sharding.create(
				new ToIntFunction<>() {
					final int count = rendezvousHashings.size();

					@Override
					public int applyAsInt(Object item) {
						return keyGetter.apply(item).hashCode() % count;
					}
				},
				rendezvousHashings);
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
