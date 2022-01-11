package adder;

import discovery.ConfigDiscoveryService;
import io.activej.async.service.EventloopTaskScheduler;
import io.activej.config.Config;
import io.activej.crdt.CrdtServer;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.cluster.CrdtRepartitionController;
import io.activej.crdt.storage.cluster.CrdtStorageCluster;
import io.activej.crdt.storage.cluster.DiscoveryService;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.inject.Key;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;

import java.time.Duration;
import java.util.List;

import static io.activej.common.Checks.checkArgument;
import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.serializer.BinarySerializers.LONG_SERIALIZER;

public final class ClusterStorageModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(new Key<CrdtStorage<Long, DetailedSumsCrdtState>>() {})
				.to(new Key<CrdtStorageCluster<Long, DetailedSumsCrdtState, String>>() {});
	}

	@Provides
	CrdtDataSerializer<Long, DetailedSumsCrdtState> serializer() {
		return new CrdtDataSerializer<>(LONG_SERIALIZER, DetailedSumsCrdtState.SERIALIZER);
	}

	@Provides
	CrdtStorageCluster<Long, DetailedSumsCrdtState, String> clusterStorage(
			Eventloop eventloop,
			DiscoveryService<Long, DetailedSumsCrdtState, String> discoveryService,
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
				.withListenAddress(config.get(ofInetSocketAddress(), "crdt.listenAddresses"));
	}

	@Provides
	DiscoveryService<Long, DetailedSumsCrdtState, String> discoveryService(
			Eventloop eventloop,
			CrdtDataSerializer<Long, DetailedSumsCrdtState> serializer,
			@Local CrdtStorage<Long, DetailedSumsCrdtState> localStorage,
			Config config
	) {
		return ConfigDiscoveryService.create(eventloop, localStorage, serializer, config.getChild("crdt.cluster"));
	}

	@Provides
	@Local
	String localId(Config config) {
		List<String> addresses = config.getChild("crdt.cluster").get(ofList(ofString()), "addresses");

		for (String addressString : addresses) {
			int splitIdx = addressString.lastIndexOf('=');
			checkArgument(splitIdx != -1, "Wrong address format");

			String id = addressString.substring(0, splitIdx);
			String address = addressString.substring(splitIdx + 1);

			if (address.equals("local")) {
				return id;
			}
		}

		throw new IllegalStateException("No 'local' address is found in config");
	}

	@Provides
	CrdtRepartitionController<Long, DetailedSumsCrdtState, String> repartitionController(
			CrdtStorageCluster<Long, DetailedSumsCrdtState, String> cluster,
			@Local String localId
	) {
		return CrdtRepartitionController.create(cluster, localId);
	}

	@Provides
	@Eager
	@Named("Repartition")
	EventloopTaskScheduler repartitionController(Eventloop eventloop, CrdtRepartitionController<Long, DetailedSumsCrdtState, String> repartitionController) {
		return EventloopTaskScheduler.create(eventloop, repartitionController::repartition)
				.withInterval(Duration.ofSeconds(10));
	}
}
