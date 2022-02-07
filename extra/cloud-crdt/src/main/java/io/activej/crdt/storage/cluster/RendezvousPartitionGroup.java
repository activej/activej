package io.activej.crdt.storage.cluster;

import io.activej.common.initializer.WithInitializer;

import java.util.Set;

public final class RendezvousPartitionGroup<P> implements WithInitializer<RendezvousPartitionGroup<P>> {
	private final Set<P> partitions;
	private final int replicas;
	private final boolean repartition;
	private final boolean active;

	private RendezvousPartitionGroup(Set<P> partitions, int replicas, boolean repartition, boolean active) {
		this.partitions = partitions;
		this.replicas = replicas;
		this.repartition = repartition;
		this.active = active;
	}

	public static <P> RendezvousPartitionGroup<P> create(Set<P> set, int replicas, boolean repartition, boolean active) {
		return new RendezvousPartitionGroup<>(set, replicas, repartition, active);
	}

	public static <P> RendezvousPartitionGroup<P> create(Set<P> set) {
		return new RendezvousPartitionGroup<>(set, 1, false, true);
	}

	public RendezvousPartitionGroup<P> withReplicas(int replicas) {
		return new RendezvousPartitionGroup<>(partitions, replicas, repartition, active);
	}

	public RendezvousPartitionGroup<P> withRepartition(boolean repartition) {
		return new RendezvousPartitionGroup<>(partitions, replicas, repartition, active);
	}

	public RendezvousPartitionGroup<P> withActive(boolean active) {
		return new RendezvousPartitionGroup<>(partitions, replicas, repartition, active);
	}

	public Set<P> getPartitions() {
		return partitions;
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
