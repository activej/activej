package adder;

import io.activej.async.service.EventloopTaskScheduler;
import io.activej.config.Config;
import io.activej.crdt.CrdtServer;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.cluster.*;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.inject.Key;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.launchers.crdt.ConfigConverters;
import io.activej.launchers.crdt.Local;

import java.time.Duration;

import static io.activej.launchers.crdt.ConfigConverters.ofSimplePartitionId;
import static io.activej.serializer.BinarySerializers.LONG_SERIALIZER;

public final class ClusterStorageModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(new Key<CrdtStorage<Long, DetailedSumsCrdtState>>() {})
				.to(new Key<CrdtStorageCluster<Long, DetailedSumsCrdtState, SimplePartitionId>>() {});
	}

	@Provides
	CrdtDataSerializer<Long, DetailedSumsCrdtState> serializer() {
		return new CrdtDataSerializer<>(LONG_SERIALIZER, DetailedSumsCrdtState.SERIALIZER);
	}

	@Provides
	CrdtStorageCluster<Long, DetailedSumsCrdtState, SimplePartitionId> clusterStorage(
			Eventloop eventloop,
			DiscoveryService<SimplePartitionId> discoveryService,
			CrdtFunction<DetailedSumsCrdtState> crdtFunction
	) {
		return CrdtStorageCluster.create(eventloop, discoveryService, crdtFunction);
	}

	@Provides
	@Eager
	CrdtServer<Long, DetailedSumsCrdtState> crdtServer(
			Eventloop eventloop,
			SimplePartitionId partitionId,
			@Local CrdtStorage<Long, DetailedSumsCrdtState> localStorage,
			CrdtDataSerializer<Long, DetailedSumsCrdtState> serializer,
			Config config
	) {
		return CrdtServer.create(eventloop, localStorage, serializer)
				.withListenPort(partitionId.getCrdtPort());
	}

	@Provides
	DiscoveryService<SimplePartitionId> discoveryService(
			Eventloop eventloop,
			CrdtDataSerializer<Long, DetailedSumsCrdtState> serializer,
			SimplePartitionId localId,
			@Local CrdtStorage<Long, DetailedSumsCrdtState> localStorage,
			Config config
	) {
		RendezvousPartitionings<SimplePartitionId> partitionings = config.get(ConfigConverters.ofRendezvousPartitionings(eventloop, serializer, localId, localStorage), "crdt.cluster");
		return DiscoveryService.of(partitionings);
	}

	@Provides
	SimplePartitionId localId(Config config) {
		return config.get(ofSimplePartitionId(), "crdt.cluster.localPartition");
	}

	@Provides
	CrdtRepartitionController<Long, DetailedSumsCrdtState, SimplePartitionId> repartitionController(
			CrdtStorageCluster<Long, DetailedSumsCrdtState, SimplePartitionId> cluster,
			SimplePartitionId partitionId
	) {
		return CrdtRepartitionController.create(cluster, partitionId);
	}

	@Provides
	@Eager
	@Named("Repartition")
	EventloopTaskScheduler repartitionController(Eventloop eventloop, CrdtRepartitionController<Long, DetailedSumsCrdtState, SimplePartitionId> repartitionController) {
		return EventloopTaskScheduler.create(eventloop, repartitionController::repartition)
				.withInterval(Duration.ofSeconds(10));
	}
}
