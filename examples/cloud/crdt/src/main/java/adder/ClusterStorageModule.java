package adder;

import io.activej.async.service.TaskScheduler;
import io.activej.config.Config;
import io.activej.crdt.CrdtException;
import io.activej.crdt.CrdtServer;
import io.activej.crdt.CrdtStorage_Client;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.AsyncCrdtStorage;
import io.activej.crdt.storage.cluster.*;
import io.activej.crdt.util.BinarySerializer_CrdtData;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.launchers.crdt.Local;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;

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

	@Provides
	BinarySerializer_CrdtData<Long, DetailedSumsCrdtState> serializer() {
		return new BinarySerializer_CrdtData<>(LONG_SERIALIZER, DetailedSumsCrdtState.SERIALIZER);
	}

	@Provides
	CrdtStorage_Cluster<Long, DetailedSumsCrdtState, PartitionId> clusterStorage(Reactor reactor,
			AsyncDiscoveryService<PartitionId> discoveryService, CrdtFunction<DetailedSumsCrdtState> crdtFunction) {
		return CrdtStorage_Cluster.create(reactor, discoveryService, crdtFunction);
	}

	@Provides
	@Eager
	CrdtServer<Long, DetailedSumsCrdtState> crdtServer(NioReactor reactor,
			@Local AsyncCrdtStorage<Long, DetailedSumsCrdtState> localStorage, BinarySerializer_CrdtData<Long, DetailedSumsCrdtState> serializer, Config config) {
		return CrdtServer.create(reactor, localStorage, serializer)
				.withInitializer(ofAbstractServer(config.getChild("crdt.server")));
	}

	@Provides
	AsyncDiscoveryService<PartitionId> discoveryService(NioReactor reactor,
			@Local AsyncCrdtStorage<Long, DetailedSumsCrdtState> localStorage, BinarySerializer_CrdtData<Long, DetailedSumsCrdtState> serializer, Config config,
			PartitionId localPartitionId) throws CrdtException {
		Path pathToFile = config.get(ofPath(), "crdt.cluster.partitionFile", DEFAULT_PARTITIONS_FILE);
		return DiscoveryService_File.create(reactor, pathToFile)
				.withCrdtProvider(partitionId -> {
					if (partitionId.equals(localPartitionId)) return localStorage;

					InetSocketAddress crdtAddress = checkNotNull(partitionId.getCrdtAddress());
					return CrdtStorage_Client.create(reactor, crdtAddress, serializer);
				});
	}

	@Provides
	PartitionId localPartitionId(Config config) {
		return config.get(ofPartitionId(), "crdt.cluster.localPartition");
	}

	@Provides
	CrdtRepartitionController<Long, DetailedSumsCrdtState, PartitionId> repartitionController(Reactor reactor,
			CrdtStorage_Cluster<Long, DetailedSumsCrdtState, PartitionId> cluster,
			PartitionId partitionId) {
		return CrdtRepartitionController.create(reactor, cluster, partitionId);
	}

	@Provides
	@Eager
	@Named("Repartition")
	TaskScheduler repartitionController(Reactor reactor,
			CrdtRepartitionController<Long, DetailedSumsCrdtState, PartitionId> repartitionController) {
		return TaskScheduler.create(reactor, repartitionController::repartition)
				.withInterval(Duration.ofSeconds(10));
	}
}
