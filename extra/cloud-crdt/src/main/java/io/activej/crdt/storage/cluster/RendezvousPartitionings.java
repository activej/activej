package io.activej.crdt.storage.cluster;

import io.activej.common.HashUtils;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.rpc.client.sender.RpcStrategy;
import io.activej.rpc.client.sender.RpcStrategyRendezvousHashing;
import io.activej.rpc.client.sender.RpcStrategySharding;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.function.ToLongBiFunction;

import static io.activej.crdt.storage.cluster.RendezvousHashSharder.NUMBER_OF_BUCKETS;

public final class RendezvousPartitionings<K extends Comparable<K>, S, P> implements DiscoveryService.Partitionings<K, S, P> {
	private final Map<P, CrdtStorage<K, S>> partitions;
	private final List<RendezvousPartitioning<P>> partitionings;
	private final ToLongBiFunction<P, Integer> hashBucketFn;
	private final ToIntFunction<K> hashFn;

	@SuppressWarnings("unchecked")
	private RendezvousPartitionings(Map<P, ? extends CrdtStorage<K, S>> partitions, List<? extends RendezvousPartitioning<P>> partitionings, ToLongBiFunction<P, Integer> hashBucketFn, ToIntFunction<K> hashFn) {
		this.partitions = (Map<P, CrdtStorage<K, S>>) partitions;
		this.partitionings = (List<RendezvousPartitioning<P>>) partitionings;
		this.hashBucketFn = hashBucketFn;
		this.hashFn = hashFn;
	}

	@SafeVarargs
	public static <K extends Comparable<K>, S, P> RendezvousPartitionings<K, S, P> create(Map<P, ? extends CrdtStorage<K, S>> partitions,
			RendezvousPartitioning<P>... partitionings) {
		return create(partitions, Arrays.asList(partitionings));
	}

	public static <K extends Comparable<K>, S, P> RendezvousPartitionings<K, S, P> create(Map<P, ? extends CrdtStorage<K, S>> partitions,
			List<RendezvousPartitioning<P>> partitionings) {
		return new RendezvousPartitionings<>(partitions, partitionings,
				defaultHashBucketFn(), Objects::hashCode);
	}

	public static <P> ToLongBiFunction<P, Integer> defaultHashBucketFn() {
		return defaultHashBucketFn(Object::hashCode);
	}

	public static <P> ToLongBiFunction<P, Integer> defaultHashBucketFn(ToIntFunction<P> partitionHashCode) {
		return (pid, bucket) -> HashUtils.murmur3hash(((long) partitionHashCode.applyAsInt(pid) << 32) | (bucket & 0xFFFFFFFFL));
	}

	public RendezvousPartitionings<K, S, P> withPartitioning(RendezvousPartitioning<P> partitioning) {
		List<RendezvousPartitioning<P>> partitionings = new ArrayList<>(this.partitionings);
		partitionings.add(partitioning);
		return new RendezvousPartitionings<>(partitions, partitionings, hashBucketFn, hashFn);
	}

	public RendezvousPartitionings<K, S, P> withHashBucketFn(ToLongBiFunction<P, Integer> hashBucketFn) {
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
		for (RendezvousPartitioning<P> partitioning : partitionings) {
			RendezvousHashSharder<K, P> sharder = RendezvousHashSharder.create(hashBucketFn, hashFn,
					partitioning.getPartitions(), alive, partitioning.getReplicas(), partitioning.isRepartition());
			sharders.add(sharder);
		}
		return RendezvousHashSharder.unionOf(sharders);
	}

	@Override
	public RpcStrategy createRpcStrategy(
			Function<P, @NotNull RpcStrategy> rpcStrategyProvider, Function<Object, K> keyGetter) {

		List<RpcStrategy> rendezvousHashings = new ArrayList<>();
		for (RendezvousPartitioning<P> partitioning : partitionings) {
			if (!partitioning.isActive()) continue;
			RpcStrategyRendezvousHashing rendezvousHashing = RpcStrategyRendezvousHashing.create(req ->
							hashFn.applyAsInt(keyGetter.apply(req)))
					.withHashBuckets(NUMBER_OF_BUCKETS)
					.withHashBucketFunction(hashBucketFn);
			for (P pid : partitioning.getPartitions()) {
				rendezvousHashing.withShard(pid, rpcStrategyProvider.apply(pid));
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
