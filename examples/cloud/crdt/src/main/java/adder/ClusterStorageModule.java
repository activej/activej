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

import static io.activej.launchers.crdt.ConfigConverters.ofPartitionId;
import static io.activej.serializer.BinarySerializers.LONG_SERIALIZER;

public final class ClusterStorageModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(new Key<CrdtStorage<Long, DetailedSumsCrdtState>>() {})
				.to(new Key<CrdtStorageCluster<Long, DetailedSumsCrdtState, PartitionId>>() {});
	}

	@Provides
	CrdtDataSerializer<Long, DetailedSumsCrdtState> serializer() {
		return new CrdtDataSerializer<>(LONG_SERIALIZER, DetailedSumsCrdtState.SERIALIZER);
	}

	@Provides
	CrdtStorageCluster<Long, DetailedSumsCrdtState, PartitionId> clusterStorage(
			Eventloop eventloop,
			DiscoveryService discoveryService,
//			CrdtDataSerializer<Long, DetailedSumsCrdtState> serializer,
			CrdtFunction<DetailedSumsCrdtState> crdtFunction
//			SimplePartitionId localPartitionId,
//			@Local CrdtStorage<Long, DetailedSumsCrdtState> localCrdtStorage
	) {
		return CrdtStorageCluster.create(eventloop, discoveryService,
/*
				partitionId -> partitionId.equals(localPartitionId) ?
						localCrdtStorage :
						CrdtStorageClient.create(eventloop, ((SimpleServerId) partitionId.getServerId()).getCrdtAddress(), serializer),
*/
				crdtFunction);
	}

	@Provides
	@Eager
	CrdtServer<Long, DetailedSumsCrdtState> crdtServer(
			Eventloop eventloop,
			PartitionId partitionId,
			@Local CrdtStorage<Long, DetailedSumsCrdtState> localStorage,
			CrdtDataSerializer<Long, DetailedSumsCrdtState> serializer,
			Config config
	) {
		return CrdtServer.create(eventloop, localStorage, serializer)
				.withListenAddress(partitionId.getCrdtAddress());
	}

	@Provides
	DiscoveryService discoveryService(Config config) {
		RendezvousPartitionScheme partitionScheme = config.get(ConfigConverters.ofRendezvousPartitionScheme(ofPartitionId()), "crdt.cluster");
		return DiscoveryService.of(partitionScheme);
	}

	@Provides
	PartitionId localPartitionId(Config config) {
		return config.get(ofPartitionId(), "crdt.cluster.localPartition");
	}

	@Provides
	CrdtRepartitionController<Long, DetailedSumsCrdtState, PartitionId> repartitionController(
			CrdtStorageCluster<Long, DetailedSumsCrdtState, PartitionId> cluster,
			PartitionId partitionId
	) {
		return CrdtRepartitionController.create(cluster, partitionId);
	}

	@Provides
	@Eager
	@Named("Repartition")
	EventloopTaskScheduler repartitionController(Eventloop eventloop, CrdtRepartitionController<Long, DetailedSumsCrdtState, PartitionId> repartitionController) {
		return EventloopTaskScheduler.create(eventloop, repartitionController::repartition)
				.withInterval(Duration.ofSeconds(10));
	}
}
