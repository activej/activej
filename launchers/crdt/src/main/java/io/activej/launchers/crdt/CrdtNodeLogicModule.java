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
import io.activej.crdt.CrdtServer;
import io.activej.crdt.CrdtStorageClient;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.cluster.*;
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

import static io.activej.common.Checks.checkNotNull;
import static io.activej.launchers.crdt.ConfigConverters.ofPartitionId;
import static io.activej.launchers.crdt.ConfigConverters.ofRendezvousPartitionScheme;
import static io.activej.launchers.initializers.Initializers.ofAbstractServer;
import static io.activej.launchers.initializers.Initializers.ofEventloop;

public abstract class CrdtNodeLogicModule<K extends Comparable<K>, S> extends AbstractModule {
	@Override
	protected void configure() {
		Type genericSuperclass = getClass().getGenericSuperclass();
		Type[] typeArguments = ((ParameterizedType) genericSuperclass).getActualTypeArguments();

		@NotNull Type supertype = Types.parameterizedType(CrdtStorage.class, typeArguments);

		bind(Key.ofType(supertype, InMemory.class))
				.to(Key.ofType(Types.parameterizedType(CrdtStorageMap.class, typeArguments)));
		bind(Key.ofType(supertype, Persistent.class))
				.to(Key.ofType(Types.parameterizedType(CrdtStorageFs.class, typeArguments)));


		Type[] clusterStorageTypes = Arrays.copyOf(typeArguments, 3);
		clusterStorageTypes[2] = PartitionId.class;

		bind((Key<?>) Key.ofType(supertype, Cluster.class))
				.to(Key.ofType(Types.parameterizedType(CrdtStorageCluster.class, clusterStorageTypes)));
	}

	@Provides
	CrdtStorageMap<K, S> runtimeCrdtClient(Eventloop eventloop, CrdtDescriptor<K, S> descriptor) {
		return CrdtStorageMap.create(eventloop, descriptor.getCrdtFunction());
	}

	@Provides
	CrdtStorageFs<K, S> fsCrdtClient(Eventloop eventloop, ActiveFs activeFs, CrdtDescriptor<K, S> descriptor) {
		return CrdtStorageFs.create(eventloop, activeFs, descriptor.getSerializer(), descriptor.getCrdtFunction());
	}

	@Provides
	DiscoveryService<PartitionId> discoveryService(
			Eventloop eventloop,
			PartitionId localPartitionId,
			CrdtStorageMap<K, S> localCrdtStorage,
			CrdtDescriptor<K, S> descriptor,
			Config config
	) {
		RendezvousPartitionScheme<PartitionId> scheme = config.get(ofRendezvousPartitionScheme(ofPartitionId()), "crdt.cluster")
				.withPartitionIdGetter(PartitionId::getId)
				.withCrdtProvider(partitionId -> {
					if (partitionId.equals(localPartitionId)) return localCrdtStorage;

					InetSocketAddress crdtAddress = checkNotNull(partitionId.getCrdtAddress());
					return CrdtStorageClient.create(eventloop, crdtAddress, descriptor.getSerializer());
				});

		return DiscoveryService.of(scheme);
	}

	@Provides
	PartitionId localPartitionId(Config config) {
		return config.get(ofPartitionId(), "crdt.cluster.localPartitionId");
	}

	@Provides
	CrdtStorageCluster<K, S, PartitionId> clusterCrdtClient(
			Eventloop eventloop,
			DiscoveryService<PartitionId> discoveryService,
			CrdtDescriptor<K, S> descriptor
	) {
		return CrdtStorageCluster.create(eventloop, discoveryService, descriptor.getCrdtFunction());
	}

	@Provides
	CrdtRepartitionController<K, S, PartitionId> crdtRepartitionController(CrdtStorageCluster<K, S, PartitionId> clusterClient, Config config) {
		return CrdtRepartitionController.create(clusterClient, config.get(ofPartitionId(), "crdt.cluster.localPartitionId"));
	}

	@Provides
	CrdtServer<K, S> crdtServer(Eventloop eventloop, CrdtStorageMap<K, S> client, CrdtDescriptor<K, S> descriptor, PartitionId localPartitionId, Config config) {
		return CrdtServer.create(eventloop, client, descriptor.getSerializer())
				.withInitializer(ofAbstractServer(config.getChild("crdt.server")))
				.withListenAddress(localPartitionId.getCrdtAddress());
	}

	@Provides
	@Cluster
	CrdtServer<K, S> clusterServer(Eventloop eventloop, CrdtStorageCluster<K, S, PartitionId> client, CrdtDescriptor<K, S> descriptor, Config config) {
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
