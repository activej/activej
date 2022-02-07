package io.activej.crdt.storage.cluster;

import io.activej.common.initializer.WithInitializer;
import io.activej.rpc.client.sender.RpcStrategy;
import io.activej.rpc.client.sender.RpcStrategyRendezvousHashing;
import io.activej.rpc.client.sender.RpcStrategySharding;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import static io.activej.crdt.storage.cluster.RendezvousHashSharder.NUMBER_OF_BUCKETS;
import static java.util.stream.Collectors.toSet;

public final class RendezvousPartitionScheme<P> implements DiscoveryService.PartitionScheme<P>, WithInitializer<RendezvousPartitionScheme<P>> {
	private final List<RendezvousPartitionGroup<P>> partitionGroups;
	private final ToIntFunction<?> hashFn;

	@SuppressWarnings("unchecked")
	private RendezvousPartitionScheme(List<? extends RendezvousPartitionGroup<P>> partitionGroups, ToIntFunction<?> hashFn) {
		this.partitionGroups = (List<RendezvousPartitionGroup<P>>) partitionGroups;
		this.hashFn = hashFn;
	}

	@SafeVarargs
	public static <P> RendezvousPartitionScheme<P> create(RendezvousPartitionGroup<P>... partitionGroups) {
		return create(Arrays.asList(partitionGroups));
	}

	public static <P> RendezvousPartitionScheme<P> create(List<RendezvousPartitionGroup<P>> partitionGroups) {
		return new RendezvousPartitionScheme<>(partitionGroups,
				Objects::hashCode);
	}

	public RendezvousPartitionScheme<P> withPartitionGroup(RendezvousPartitionGroup<P> partitionGroup) {
		List<RendezvousPartitionGroup<P>> partitionGroups = new ArrayList<>(this.partitionGroups);
		partitionGroups.add(partitionGroup);
		return new RendezvousPartitionScheme<>(partitionGroups, hashFn);
	}

	public <K extends Comparable<K>> RendezvousPartitionScheme<P> withHashFn(ToIntFunction<K> hashFn) {
		return new RendezvousPartitionScheme<>(partitionGroups, hashFn);
	}

	@Override
	public Set<P> getPartitions() {
		return partitionGroups.stream().flatMap(p -> p.getPartitions().stream()).collect(toSet());
	}

	@Override
	public <K extends Comparable<K>> @Nullable Sharder<K> createSharder(List<P> alive) {
		List<RendezvousHashSharder<K, P>> sharders = new ArrayList<>();
		for (RendezvousPartitionGroup<P> partitionGroup : partitionGroups) {
			//noinspection unchecked
			RendezvousHashSharder<K, P> sharder = RendezvousHashSharder.create(
					((ToIntFunction<K>) hashFn),
					partitionGroup.getPartitions(), alive, partitionGroup.getReplicas(), partitionGroup.isRepartition());
			sharders.add(sharder);
		}
		return RendezvousHashSharder.unionOf(sharders);
	}

	@Override
	public <K extends Comparable<K>> RpcStrategy createRpcStrategy(Function<P, @NotNull RpcStrategy> provider,
			Function<Object, K> keyGetter) {

		List<RpcStrategy> rendezvousHashings = new ArrayList<>();
		for (RendezvousPartitionGroup<P> partitionGroup : partitionGroups) {
			if (!partitionGroup.isActive()) continue;
			//noinspection unchecked
			RpcStrategyRendezvousHashing rendezvousHashing = RpcStrategyRendezvousHashing.create(req ->
							((ToIntFunction<K>) hashFn).applyAsInt(keyGetter.apply(req)))
					.withHashBuckets(NUMBER_OF_BUCKETS)
					.withHashBucketFunction(RendezvousHashSharder::hashBucket);
			for (P pid : partitionGroup.getPartitions()) {
				rendezvousHashing.withShard(pid, provider.apply(pid));
			}
			if (!partitionGroup.isRepartition()) {
				rendezvousHashing = rendezvousHashing.withStrictShards(partitionGroup.getReplicas());
			}
			rendezvousHashings.add(rendezvousHashing);
		}

		return RpcStrategySharding.create(
				new ToIntFunction<Object>() {
					final int count = rendezvousHashings.size();

					@Override
					public int applyAsInt(Object item) {
						return keyGetter.apply(item).hashCode() % count;
					}
				}, rendezvousHashings);
	}

}
