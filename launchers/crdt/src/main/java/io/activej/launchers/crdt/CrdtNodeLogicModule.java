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
import io.activej.crdt.RemoteCrdtStorage;
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.crdt.storage.cluster.*;
import io.activej.crdt.storage.local.FileSystemCrdtStorage;
import io.activej.crdt.storage.local.MapCrdtStorage;
import io.activej.eventloop.Eventloop;
import io.activej.fs.IFileSystem;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.QualifierAnnotation;
import io.activej.inject.module.AbstractModule;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.types.Types;

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
import static io.activej.launchers.crdt.ConfigConverters.ofRendezvousPartitionSchemeBuilder;
import static io.activej.launchers.initializers.Initializers.ofAbstractServer;
import static io.activej.launchers.initializers.Initializers.ofEventloop;

public abstract class CrdtNodeLogicModule<K extends Comparable<K>, S> extends AbstractModule {
	@Override
	protected void configure() {
		Type genericSuperclass = getClass().getGenericSuperclass();
		Type[] typeArguments = ((ParameterizedType) genericSuperclass).getActualTypeArguments();

		Type supertype = Types.parameterizedType(ICrdtStorage.class, typeArguments);

		bind(Key.ofType(supertype, InMemory.class))
			.to(Key.ofType(Types.parameterizedType(MapCrdtStorage.class, typeArguments)));
		bind(Key.ofType(supertype, Persistent.class))
			.to(Key.ofType(Types.parameterizedType(FileSystemCrdtStorage.class, typeArguments)));
		Type[] clusterStorageTypes = Arrays.copyOf(typeArguments, 3);
		clusterStorageTypes[2] = PartitionId.class;

		bind((Key<?>) Key.ofType(supertype, Cluster.class))
			.to(Key.ofType(Types.parameterizedType(ClusterCrdtStorage.class, clusterStorageTypes)));
	}

	@Provides
	MapCrdtStorage<K, S> runtimeCrdtClient(Reactor reactor, CrdtDescriptor<K, S> descriptor) {
		return MapCrdtStorage.create(reactor, descriptor.crdtFunction());
	}

	@Provides
	FileSystemCrdtStorage<K, S> fileSystemCrdtClient(Reactor reactor, IFileSystem fileSystem, CrdtDescriptor<K, S> descriptor) {
		return FileSystemCrdtStorage.create(reactor, fileSystem, descriptor.serializer(), descriptor.crdtFunction());
	}

	@Provides
	IDiscoveryService<PartitionId> discoveryService(
		NioReactor reactor, PartitionId localPartitionId, MapCrdtStorage<K, S> localCrdtStorage,
		CrdtDescriptor<K, S> descriptor, Config config
	) {
		RendezvousPartitionScheme<PartitionId> scheme = config.get(ofRendezvousPartitionSchemeBuilder(ofPartitionId()), "crdt.cluster")
			.withPartitionIdGetter(PartitionId::getId)
			.withCrdtProvider(partitionId -> {
				if (partitionId.equals(localPartitionId)) return localCrdtStorage;

				InetSocketAddress crdtAddress = checkNotNull(partitionId.getCrdtAddress());
				return RemoteCrdtStorage.create(reactor, crdtAddress, descriptor.serializer());
			})
			.build();

		return IDiscoveryService.of(scheme);
	}

	@Provides
	PartitionId localPartitionId(Config config) {
		return config.get(ofPartitionId(), "crdt.cluster.localPartitionId");
	}

	@Provides
	ClusterCrdtStorage<K, S, PartitionId> clusterCrdtClient(
		Reactor reactor, IDiscoveryService<PartitionId> discoveryService, CrdtDescriptor<K, S> descriptor
	) {
		return ClusterCrdtStorage.create(reactor, discoveryService, descriptor.crdtFunction());
	}

	@Provides
	CrdtRepartitionController<K, S, PartitionId> crdtRepartitionController(
		Reactor reactor, ClusterCrdtStorage<K, S, PartitionId> clusterClient, Config config
	) {
		return CrdtRepartitionController.create(reactor, clusterClient, config.get(ofPartitionId(), "crdt.cluster.localPartitionId"));
	}

	@Provides
	CrdtServer<K, S> crdtServer(
		NioReactor reactor, MapCrdtStorage<K, S> client, CrdtDescriptor<K, S> descriptor, PartitionId localPartitionId,
		Config config
	) {
		return CrdtServer.builder(reactor, client, descriptor.serializer())
			.initialize(ofAbstractServer(config.getChild("crdt.server")))
			.withListenAddress(localPartitionId.getCrdtAddress())
			.build();
	}

	@Provides
	@Cluster
	CrdtServer<K, S> clusterServer(
		NioReactor reactor, ClusterCrdtStorage<K, S, PartitionId> client, CrdtDescriptor<K, S> descriptor,
		Config config
	) {
		return CrdtServer.builder(reactor, client, descriptor.serializer())
			.initialize(ofAbstractServer(config.getChild("crdt.cluster.server")))
			.build();
	}

	@Provides
	NioReactor reactor(Config config) {
		return Eventloop.builder()
			.initialize(ofEventloop(config))
			.build();
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
