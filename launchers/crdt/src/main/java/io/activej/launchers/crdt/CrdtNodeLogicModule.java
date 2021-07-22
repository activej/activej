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

import io.activej.common.reflection.RecursiveType;
import io.activej.config.Config;
import io.activej.crdt.CrdtServer;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.cluster.CrdtPartitions;
import io.activej.crdt.storage.cluster.CrdtRepartitionController;
import io.activej.crdt.storage.cluster.CrdtStorageCluster;
import io.activej.crdt.storage.local.CrdtStorageFs;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.eventloop.Eventloop;
import io.activej.fs.ActiveFs;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.QualifierAnnotation;
import io.activej.inject.module.AbstractModule;
import org.jetbrains.annotations.NotNull;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

import static io.activej.launchers.crdt.Initializers.ofCrdtCluster;
import static io.activej.launchers.crdt.Initializers.ofFsCrdtClient;
import static io.activej.launchers.initializers.Initializers.ofAbstractServer;
import static io.activej.launchers.initializers.Initializers.ofEventloop;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

public abstract class CrdtNodeLogicModule<K extends Comparable<K>, S> extends AbstractModule {
	@Override
	protected void configure() {
		Type genericSuperclass = getClass().getGenericSuperclass();
		Type[] typeArguments = ((ParameterizedType) genericSuperclass).getActualTypeArguments();

		List<RecursiveType> typeArgs = Arrays.stream(typeArguments).map(RecursiveType::of).collect(toList());
		@NotNull Type supertype = RecursiveType.of(CrdtStorage.class, typeArgs).getType();

		bind((Key<?>) Key.ofType(supertype, InMemory.class))
				.to(Key.ofType(RecursiveType.of(CrdtStorageMap.class, typeArgs).getType()));
		bind((Key<?>) Key.ofType(supertype, Persistent.class))
				.to(Key.ofType(RecursiveType.of(CrdtStorageFs.class, typeArgs).getType()));

		bind((Key<?>) Key.ofType(supertype, Cluster.class))
				.to(Key.ofType(RecursiveType.of(CrdtStorageCluster.class, typeArgs).getType()));
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
	CrdtPartitions<K, S> partitions(Eventloop eventloop, CrdtStorageMap<K, S> localClient, Config config){
		return CrdtPartitions.create(eventloop, singletonMap(config.get("crdt.cluster.localPartitionId"), localClient));
	}

	@Provides
	CrdtStorageCluster<K, S> clusterCrdtClient(Config config, CrdtPartitions<K, S> partitions, CrdtStorageMap<K, S> localClient, CrdtDescriptor<K, S> descriptor) {
		return CrdtStorageCluster.create(
				partitions,
				descriptor.getCrdtFunction())
				.withInitializer(ofCrdtCluster(config.getChild("crdt.cluster"), localClient, descriptor));
	}

	@Provides
	CrdtRepartitionController<K, S> crdtRepartitionController(CrdtStorageCluster<K, S> clusterClient, Config config) {
		return CrdtRepartitionController.create(clusterClient, config.get("crdt.cluster.localPartitionId"));
	}

	@Provides
	CrdtServer<K, S> crdtServer(Eventloop eventloop, CrdtStorageMap<K, S> client, CrdtDescriptor<K, S> descriptor, Config config) {
		return CrdtServer.create(eventloop, client, descriptor.getSerializer())
				.withInitializer(ofAbstractServer(config.getChild("crdt.server")));
	}

	@Provides
	@Cluster
	CrdtServer<K, S> clusterServer(Eventloop eventloop, CrdtStorageCluster<K, S> client, CrdtDescriptor<K, S> descriptor, Config config) {
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
