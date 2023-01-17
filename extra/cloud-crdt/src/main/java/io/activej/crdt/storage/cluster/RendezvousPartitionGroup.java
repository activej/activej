package io.activej.crdt.storage.cluster;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import io.activej.common.initializer.AbstractBuilder;

import java.util.Set;

public final class RendezvousPartitionGroup<P> {
	private final Set<P> partitionIds;
	private int replicaCount;
	private boolean repartition;
	private boolean active;

	@CompiledJson
	RendezvousPartitionGroup(Set<P> partitionIds, int replicaCount, boolean repartition, boolean active) {
		this.partitionIds = partitionIds;
		this.replicaCount = replicaCount;
		this.repartition = repartition;
		this.active = active;
	}

	public static <P> RendezvousPartitionGroup<P> create(Set<P> serverIds, int replicas, boolean repartition, boolean active) {
		return builder(serverIds)
				.withReplicas(replicas)
				.withRepartition(repartition)
				.withActive(active)
				.build();
	}

	public static <P> RendezvousPartitionGroup<P> create(Set<P> serverIds) {
		return builder(serverIds)
				.withReplicas(1)
				.withActive(true)
				.build();
	}

	public static <P> RendezvousPartitionGroup<P>.Builder builder(Set<P> serverIds) {
		return new RendezvousPartitionGroup<>(serverIds, 0, false, false).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, RendezvousPartitionGroup<P>> {
		private Builder() {
		}

		public Builder withReplicas(int replicaCount) {
			checkNotBuilt(this);
			RendezvousPartitionGroup.this.replicaCount = replicaCount;
			return this;
		}

		public Builder withRepartition(boolean repartition) {
			checkNotBuilt(this);
			RendezvousPartitionGroup.this.repartition = repartition;
			return this;
		}

		public Builder withActive(boolean active) {
			checkNotBuilt(this);
			RendezvousPartitionGroup.this.active = active;
			return this;
		}

		@Override
		protected RendezvousPartitionGroup<P> doBuild() {
			return RendezvousPartitionGroup.this;
		}
	}

	@JsonAttribute(name = "ids")
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
