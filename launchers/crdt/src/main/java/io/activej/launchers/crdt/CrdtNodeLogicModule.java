/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.launchers.crdt;

import io.activej.config.Config;
import io.activej.config.converter.ConfigConverters;
import io.activej.crdt.CrdtServer;
import io.activej.crdt.CrdtStorageClient;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.cluster.CrdtRepartitionController;
import io.activej.crdt.storage.cluster.CrdtStorageCluster;
import io.activej.crdt.storage.cluster.DiscoveryService;
import io.activej.crdt.storage.cluster.RendezvousPartitionings;
import io.activej.crdt.storage.local.CrdtStorageFs;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.eventloop.Eventloop;
import io.activej.fs.ActiveFs;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.QualifierAnnotation;
import io.activej.inject.module.AbstractModule;
import io.activej.types.Types;
import org.jetbrains.annotations.NotNull;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.activej.common.Checks.checkState;
import static io.activej.config.converter.ConfigConverters.ofInteger;
import static io.activej.launchers.crdt.Initializers.ofFsCrdtClient;
import static io.activej.launchers.initializers.Initializers.ofAbstractServer;
import static io.activej.launchers.initializers.Initializers.ofEventloop;

public abstract class CrdtNodeLogicModule<K extends Comparable<K>, S> extends AbstractModule {
	@Override
	protected void configure() {
		Type genericSuperclass = getClass().getGenericSuperclass();
		Type[] typeArguments = ((ParameterizedType) genericSuperclass).getActualTypeArguments();

		@NotNull Type supertype = Types.parameterizedType(CrdtStorage.class, typeArguments);

		bind((Key<?>) Key.ofType(supertype, InMemory.class))
				.to(Key.ofType(Types.parameterizedType(CrdtStorageMap.class, typeArguments)));
		bind((Key<?>) Key.ofType(supertype, Persistent.class))
				.to(Key.ofType(Types.parameterizedType(CrdtStorageFs.class, typeArguments)));

		Type[] clusterStorageTypes = Arrays.copyOf(typeArguments, 3);
		clusterStorageTypes[2] = String.class;

		bind((Key<?>) Key.ofType(supertype, Cluster.class))
				.to(Key.ofType(Types.parameterizedType(CrdtStorageCluster.class, clusterStorageTypes)));
	}

	@Provides
	CrdtStorageMap<K, S> runtimeCrdtClient(Eventloop eventloop, CrdtDescriptor<K, S> descriptor) {
		return CrdtStorageMap.create(eventloop, descriptor.getCrdtFunction());
	}

	@Provides
	CrdtStorageFs<K, S> fsCrdtClient(Eventloop eventloop, Config config, ActiveFs activeFs, CrdtDescriptor<K, S> descriptor) {
		return CrdtStorageFs.create(eventloop, activeFs, descriptor.getSerializer(), descriptor.getCrdtFunction())
				.withInitializer(ofFsCrdtClient(config));
	}

	@Provides
	DiscoveryService<K, S, String> discoveryService(CrdtStorageMap<K, S> localClient, CrdtDescriptor<K, S> descriptor, Config config) {
		config = config.getChild("crdt.cluster");

		Eventloop eventloop = localClient.getEventloop();

		Map<String, CrdtStorage<K, S>> partitions = new HashMap<>();
		partitions.put(config.get("localPartitionId"), localClient);

		Map<String, Config> partitionsConfigmap = config.getChild("partitions").getChildren();
		checkState(!partitionsConfigmap.isEmpty(), "Cluster could not operate without partitions, config had none");

		for (Map.Entry<String, Config> entry : partitionsConfigmap.entrySet()) {
			InetSocketAddress address = ConfigConverters.ofInetSocketAddress().get(entry.getValue());
			partitions.put(entry.getKey(), CrdtStorageClient.create(eventloop, address, descriptor.getSerializer()));
		}

		return DiscoveryService.of(
				RendezvousPartitionings.create(partitions)
						.withPartitioning(RendezvousPartitionings.Partitioning.create(partitions.keySet())
								.withReplicas(config.get(ofInteger(), "crdt.cluster.replicationCount", 1))));
	}

	@Provides
	CrdtStorageCluster<K, S, String> clusterCrdtClient(Eventloop eventloop, DiscoveryService<K, S, String> discoveryService, CrdtDescriptor<K, S> descriptor, Config config) {
		return CrdtStorageCluster.create(eventloop,
						discoveryService,
						descriptor.getCrdtFunction());
	}

	@Provides
	CrdtRepartitionController<K, S, String> crdtRepartitionController(CrdtStorageCluster<K, S, String> clusterClient, Config config) {
		return CrdtRepartitionController.create(clusterClient, config.get("crdt.cluster.localPartitionId"));
	}

	@Provides
	CrdtServer<K, S> crdtServer(Eventloop eventloop, CrdtStorageMap<K, S> client, CrdtDescriptor<K, S> descriptor, Config config) {
		return CrdtServer.create(eventloop, client, descriptor.getSerializer())
				.withInitializer(ofAbstractServer(config.getChild("crdt.server")));
	}

	@Provides
	@Cluster
	CrdtServer<K, S> clusterServer(Eventloop eventloop, CrdtStorageCluster<K, S, String> client, CrdtDescriptor<K, S> descriptor, Config config) {
		return CrdtServer.create(eventloop, client, descriptor.getSerializer())
				.withInitializer(ofAbstractServer(config.getChild("crdt.cluster.server")));
	}

	@Provides
	Eventloop eventloop(Config config) {
		return Eventloop.create()
				.withInitializer(ofEventloop(config));
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
	@QualifierAnnotation
	public @interface InMemory {}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
	@QualifierAnnotation
	public @interface Persistent {}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
	@QualifierAnnotation
	public @interface Cluster {}
}
