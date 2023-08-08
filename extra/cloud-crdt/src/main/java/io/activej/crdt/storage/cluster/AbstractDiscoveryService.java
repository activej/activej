package io.activej.crdt.storage.cluster;

import io.activej.common.builder.AbstractBuilder;
import io.activej.common.exception.MalformedDataException;
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.rpc.client.sender.strategy.RpcStrategy;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.function.Function;

import static io.activej.crdt.json.JsonCodecs.ofPartitionId;
import static io.activej.crdt.json.JsonCodecs.ofRendezvousPartitionGroup;
import static io.activej.json.JsonCodecs.ofList;
import static io.activej.json.JsonUtils.fromJsonBytes;

public abstract class AbstractDiscoveryService extends AbstractReactive
	implements IDiscoveryService<PartitionId> {

	protected @Nullable Function<PartitionId, RpcStrategy> rpcProvider;
	protected @Nullable Function<PartitionId, ICrdtStorage<?, ?>> crdtProvider;

	protected AbstractDiscoveryService(Reactor reactor) {
		super(reactor);
	}

	@SuppressWarnings("unchecked")
	public abstract class Builder<Self extends Builder<Self, D>, D extends AbstractDiscoveryService>
		extends AbstractBuilder<Self, D> {

		public final Self withCrdtProvider(Function<PartitionId, ICrdtStorage<?, ?>> crdtProvider) {
			AbstractDiscoveryService.this.crdtProvider = crdtProvider;
			return (Self) this;
		}

		public final Self withRpcProvider(Function<PartitionId, RpcStrategy> rpcProvider) {
			AbstractDiscoveryService.this.rpcProvider = rpcProvider;
			return (Self) this;
		}

		@Override
		protected D doBuild() {
			return (D) AbstractDiscoveryService.this;
		}
	}

	protected final RendezvousPartitionScheme<PartitionId> parseScheme(byte[] bytes) throws MalformedDataException {
		List<RendezvousPartitionGroup<PartitionId>> partitionGroups = fromJsonBytes(
			ofList(ofRendezvousPartitionGroup(ofPartitionId())),
			bytes);
		RendezvousPartitionScheme<PartitionId>.Builder schemeBuilder = RendezvousPartitionScheme.builder(partitionGroups)
			.withPartitionIdGetter(PartitionId::getId);

		if (rpcProvider != null) schemeBuilder.withRpcProvider(rpcProvider);
		if (crdtProvider != null) schemeBuilder.withCrdtProvider(crdtProvider);

		return schemeBuilder.build();
	}
}
