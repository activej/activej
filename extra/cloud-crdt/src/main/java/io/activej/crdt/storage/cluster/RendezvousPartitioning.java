package io.activej.crdt.storage.cluster;

import java.util.Set;

public final class RendezvousPartitioning<P> {
	private final Set<P> partitions;
	private final int replicas;
	private final boolean repartition;
	private final boolean active;

	private RendezvousPartitioning(Set<P> partitions, int replicas, boolean repartition, boolean active) {
		this.partitions = partitions;
		this.replicas = replicas;
		this.repartition = repartition;
		this.active = active;
	}

	public static <P> RendezvousPartitioning<P> create(Set<P> set, int replicas, boolean repartition, boolean active) {
		return new RendezvousPartitioning<>(set, replicas, repartition, active);
	}

	public static <P> RendezvousPartitioning<P> create(Set<P> set) {
		return new RendezvousPartitioning<>(set, 1, false, true);
	}

	public RendezvousPartitioning<P> withReplicas(int replicas) {
		return new RendezvousPartitioning<>(partitions, replicas, repartition, active);
	}

	public RendezvousPartitioning<P> withRepartition(boolean repartition) {
		return new RendezvousPartitioning<>(partitions, replicas, repartition, active);
	}

	public RendezvousPartitioning<P> withActive(boolean active) {
		return new RendezvousPartitioning<>(partitions, replicas, repartition, active);
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
