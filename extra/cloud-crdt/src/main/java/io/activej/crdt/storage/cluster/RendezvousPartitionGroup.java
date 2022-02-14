package io.activej.crdt.storage.cluster;

import io.activej.common.initializer.WithInitializer;

import java.util.Set;

public final class RendezvousPartitionGroup<P> implements WithInitializer<RendezvousPartitionGroup<P>> {
	private final Set<P> partitionIds;
	private int replicaCount;
	private boolean repartition;
	private boolean active;

	private RendezvousPartitionGroup(Set<P> partitionIds, int replicaCount, boolean repartition, boolean active) {
		this.partitionIds = partitionIds;
		this.replicaCount = replicaCount;
		this.repartition = repartition;
		this.active = active;
	}

	public static <P> RendezvousPartitionGroup<P> create(Set<P> serverIds, int replicas, boolean repartition, boolean active) {
		return new RendezvousPartitionGroup<>(serverIds, replicas, repartition, active);
	}

	public static <P> RendezvousPartitionGroup<P> create(Set<P> serverIds) {
		return new RendezvousPartitionGroup<>(serverIds, 1, false, true);
	}

	public RendezvousPartitionGroup<P> withReplicas(int replicaCount) {
		this.replicaCount = replicaCount;
		return this;
	}

	public RendezvousPartitionGroup<P> withRepartition(boolean repartition) {
		this.repartition = repartition;
		return this;
	}

	public RendezvousPartitionGroup<P> withActive(boolean active) {
		this.active = active;
		return this;
	}

	public Set<P> getPartitionIds() {
		return partitionIds;
	}

	public int getReplicaCount() {
		return replicaCount;
	}

	public boolean isRepartition() {
		return repartition;
	}

	public boolean isActive() {
		return active;
	}
}
