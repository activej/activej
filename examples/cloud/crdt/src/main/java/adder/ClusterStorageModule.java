package adder;

import io.activej.async.service.TaskScheduler;
import io.activej.config.Config;
import io.activej.crdt.CrdtException;
import io.activej.crdt.CrdtServer;
import io.activej.crdt.RemoteCrdtStorage;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.crdt.storage.cluster.*;
import io.activej.crdt.util.CrdtDataBinarySerializer;
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
	CrdtDataBinarySerializer<Long, DetailedSumsCrdtState> serializer() {
		return new CrdtDataBinarySerializer<>(LONG_SERIALIZER, DetailedSumsCrdtState.SERIALIZER);
	}

	@Provides
	ClusterCrdtStorage<Long, DetailedSumsCrdtState, PartitionId> clusterStorage(Reactor reactor,
			IDiscoveryService<PartitionId> discoveryService, CrdtFunction<DetailedSumsCrdtState> crdtFunction) {
		return ClusterCrdtStorage.create(reactor, discoveryService, crdtFunction);
	}

	@Provides
	@Eager
	CrdtServer<Long, DetailedSumsCrdtState> crdtServer(NioReactor reactor,
			@Local ICrdtStorage<Long, DetailedSumsCrdtState> localStorage, CrdtDataBinarySerializer<Long, DetailedSumsCrdtState> serializer, Config config) {
		return CrdtServer.builder(reactor, localStorage, serializer)
				.initialize(ofAbstractServer(config.getChild("crdt.server")))
				.build();
	}

	@Provides
	IDiscoveryService<PartitionId> discoveryService(NioReactor reactor,
			@Local ICrdtStorage<Long, DetailedSumsCrdtState> localStorage, CrdtDataBinarySerializer<Long, DetailedSumsCrdtState> serializer, Config config,
			PartitionId localPartitionId) throws CrdtException {
		Path pathToFile = config.get(ofPath(), "crdt.cluster.partitionFile", DEFAULT_PARTITIONS_FILE);
		return FileDiscoveryService.builder(reactor, pathToFile)
				.withCrdtProvider(partitionId -> {
					if (partitionId.equals(localPartitionId)) return localStorage;

					InetSocketAddress crdtAddress = checkNotNull(partitionId.getCrdtAddress());
					return RemoteCrdtStorage.create(reactor, crdtAddress, serializer);
				})
				.build();
	}

	@Provides
	PartitionId localPartitionId(Config config) {
		return config.get(ofPartitionId(), "crdt.cluster.localPartition");
	}

	@Provides
	CrdtRepartitionController<Long, DetailedSumsCrdtState, PartitionId> repartitionController(Reactor reactor,
			ClusterCrdtStorage<Long, DetailedSumsCrdtState, PartitionId> cluster,
			PartitionId partitionId) {
		return CrdtRepartitionController.create(reactor, cluster, partitionId);
	}

	@Provides
	@Eager
	@Named("Repartition")
	TaskScheduler repartitionController(Reactor reactor,
			CrdtRepartitionController<Long, DetailedSumsCrdtState, PartitionId> repartitionController) {
		return TaskScheduler.builder(reactor, repartitionController::repartition)
				.withInterval(Duration.ofSeconds(10))
				.build();
	}
}
