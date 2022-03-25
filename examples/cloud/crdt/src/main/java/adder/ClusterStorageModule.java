package adder;

import io.activej.async.service.EventloopTaskScheduler;
import io.activej.config.Config;
import io.activej.crdt.CrdtException;
import io.activej.crdt.CrdtServer;
import io.activej.crdt.CrdtStorageClient;
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
import io.activej.launchers.crdt.Local;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.config.converter.ConfigConverters.ofPath;
import static io.activej.launchers.crdt.ConfigConverters.ofPartitionId;
import static io.activej.launchers.initializers.Initializers.ofAbstractServer;
import static io.activej.serializer.BinarySerializers.LONG_SERIALIZER;

public final class ClusterStorageModule extends AbstractModule {
	public static final Path DEFAULT_PARTITIONS_FILE = Paths.get("adder-partitions.json");

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
			DiscoveryService<PartitionId> discoveryService,
			CrdtFunction<DetailedSumsCrdtState> crdtFunction
	) {
		return CrdtStorageCluster.create(eventloop, discoveryService, crdtFunction);
	}

	@Provides
	@Eager
	CrdtServer<Long, DetailedSumsCrdtState> crdtServer(
			Eventloop eventloop,
			@Local CrdtStorage<Long, DetailedSumsCrdtState> localStorage,
			CrdtDataSerializer<Long, DetailedSumsCrdtState> serializer,
			Config config
	) {
		return CrdtServer.create(eventloop, localStorage, serializer)
				.withInitializer(ofAbstractServer(config.getChild("crdt.server")));
	}

	@Provides
	DiscoveryService<PartitionId> discoveryService(
			Eventloop eventloop,
			PartitionId localPartitionId,
			@Local CrdtStorage<Long, DetailedSumsCrdtState> localStorage,
			CrdtDataSerializer<Long, DetailedSumsCrdtState> serializer,
			Config config
	) throws CrdtException {
		Path pathToFile = config.get(ofPath(), "crdt.cluster.partitionFile", DEFAULT_PARTITIONS_FILE);
		return FileDiscoveryService.create(eventloop, pathToFile)
				.withCrdtProvider(partitionId -> {
					if (partitionId.equals(localPartitionId)) return localStorage;

					InetSocketAddress crdtAddress = checkNotNull(partitionId.getCrdtAddress());
					return CrdtStorageClient.create(eventloop, crdtAddress, serializer);
				});
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
