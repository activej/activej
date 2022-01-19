package io.activej.crdt.storage.cluster;

import io.activej.common.HashUtils;
import io.activej.common.initializer.WithInitializer;
import io.activej.crdt.storage.CrdtStorage;
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

public final class RendezvousPartitionings<P> implements DiscoveryService.Partitionings<P>, WithInitializer<RendezvousPartitionings<P>> {
	private final List<RendezvousPartitioning<P>> partitionings;
	private final ToIntFunction<?> hashFn;

	@SuppressWarnings("unchecked")
	private RendezvousPartitionings(List<? extends RendezvousPartitioning<P>> partitionings, ToIntFunction<?> hashFn) {
		this.partitionings = (List<RendezvousPartitioning<P>>) partitionings;
		this.hashFn = hashFn;
	}

	@SafeVarargs
	public static <P> RendezvousPartitionings<P> create(RendezvousPartitioning<P>... partitionings) {
		return create(Arrays.asList(partitionings));
	}

	public static <P> RendezvousPartitionings<P> create(List<RendezvousPartitioning<P>> partitionings) {
		return new RendezvousPartitionings<>(partitionings,
				Objects::hashCode);
	}

	public static <P> long hashBucket(P pid, int bucket) {
		return HashUtils.murmur3hash(((long) pid.hashCode() << 32) | (bucket & 0xFFFFFFFFL));
	}

	public RendezvousPartitionings<P> withPartitioning(RendezvousPartitioning<P> partitioning) {
		List<RendezvousPartitioning<P>> partitionings = new ArrayList<>(this.partitionings);
		partitionings.add(partitioning);
		return new RendezvousPartitionings<>(partitionings, hashFn);
	}

	public <K extends Comparable<K>> RendezvousPartitionings<P> withHashFn(ToIntFunction<K> hashFn) {
		return new RendezvousPartitionings<>(partitionings, hashFn);
	}

	@Override
	public Set<P> getPartitions() {
		return partitionings.stream().flatMap(p -> p.getPartitions().stream()).collect(toSet());
	}

	@Override
	public <K extends Comparable<K>, S> @Nullable Sharder<K> createSharder(Function<P, CrdtStorage<K, S>> provider,
			List<P> alive) {
		List<RendezvousHashSharder<K, P>> sharders = new ArrayList<>();
		for (RendezvousPartitioning<P> partitioning : partitionings) {
			//noinspection unchecked
			RendezvousHashSharder<K, P> sharder = RendezvousHashSharder.create(
					((ToIntFunction<K>) hashFn),
					partitioning.getPartitions(), alive, partitioning.getReplicas(), partitioning.isRepartition());
			sharders.add(sharder);
		}
		return RendezvousHashSharder.unionOf(sharders);
	}

	@Override
	public <K extends Comparable<K>> RpcStrategy createRpcStrategy(Function<P, @NotNull RpcStrategy> provider,
			Function<Object, K> keyGetter) {

		List<RpcStrategy> rendezvousHashings = new ArrayList<>();
		for (RendezvousPartitioning<P> partitioning : partitionings) {
			if (!partitioning.isActive()) continue;
			//noinspection unchecked
			RpcStrategyRendezvousHashing rendezvousHashing = RpcStrategyRendezvousHashing.create(req ->
							((ToIntFunction<K>) hashFn).applyAsInt(keyGetter.apply(req)))
					.withHashBuckets(NUMBER_OF_BUCKETS)
					.withHashBucketFunction(RendezvousPartitionings::hashBucket);
			for (P pid : partitioning.getPartitions()) {
				rendezvousHashing.withShard(pid, provider.apply(pid));
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
