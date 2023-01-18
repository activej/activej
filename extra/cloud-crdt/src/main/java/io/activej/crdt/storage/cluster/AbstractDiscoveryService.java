package io.activej.crdt.storage.cluster;

import io.activej.common.exception.MalformedDataException;
import io.activej.common.builder.AbstractBuilder;
import io.activej.crdt.storage.AsyncCrdtStorage;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.rpc.client.sender.RpcStrategy;
import io.activej.types.TypeT;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.function.Function;

import static io.activej.fs.util.JsonUtils.fromJson;

public abstract class AbstractDiscoveryService extends AbstractReactive
		implements AsyncDiscoveryService<PartitionId> {
	protected static final TypeT<List<RendezvousPartitionGroup<PartitionId>>> PARTITION_GROUPS_TYPE = new TypeT<>() {};

	protected @Nullable Function<PartitionId, RpcStrategy> rpcProvider;
	protected @Nullable Function<PartitionId, AsyncCrdtStorage<?, ?>> crdtProvider;

	protected AbstractDiscoveryService(Reactor reactor) {
		super(reactor);
	}

	@SuppressWarnings("unchecked")
	public abstract class Builder<Self extends Builder<Self, D>, D extends AbstractDiscoveryService>
			extends AbstractBuilder<Self, D> {

		public Self withCrdtProvider(Function<PartitionId, AsyncCrdtStorage<?, ?>> crdtProvider) {
			AbstractDiscoveryService.this.crdtProvider = crdtProvider;
			return (Self) this;
		}

		public Self withRpcProvider(Function<PartitionId, RpcStrategy> rpcProvider) {
			AbstractDiscoveryService.this.rpcProvider = rpcProvider;
			return (Self) this;
		}

		@Override
		protected D doBuild() {
			return (D) AbstractDiscoveryService.this;
		}
	}

	protected final PartitionScheme_Rendezvous<PartitionId> parseScheme(byte[] bytes) throws MalformedDataException {
		List<RendezvousPartitionGroup<PartitionId>> partitionGroups = fromJson(PARTITION_GROUPS_TYPE, bytes);
		PartitionScheme_Rendezvous<PartitionId>.Builder schemeBuilder = PartitionScheme_Rendezvous.builder(partitionGroups)
				.withPartitionIdGetter(PartitionId::getId);

		if (rpcProvider != null) schemeBuilder.withRpcProvider(rpcProvider);
		if (crdtProvider != null) schemeBuilder.withCrdtProvider(crdtProvider);

		return schemeBuilder.build();
	}
}
