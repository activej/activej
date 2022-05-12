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

package io.activej.crdt.storage.cluster;

import io.activej.async.function.AsyncSupplier;
import io.activej.common.exception.MalformedDataException;
import io.activej.crdt.CrdtException;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.fs.cluster.EtcdWatchService;
import io.activej.rpc.client.sender.RpcStrategy;
import io.activej.types.TypeT;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.function.Function;

import static io.activej.crdt.util.Utils.fromJson;

public final class EtcdDiscoveryService implements DiscoveryService<PartitionId> {
	private static final TypeT<List<RendezvousPartitionGroup<PartitionId>>> PARTITION_GROUPS_TYPE = new TypeT<>() {};

	private final EtcdWatchService watchService;

	private @Nullable Function<PartitionId, @NotNull RpcStrategy> rpcProvider;
	private @Nullable Function<PartitionId, @NotNull CrdtStorage<?, ?>> crdtProvider;

	private EtcdDiscoveryService(EtcdWatchService watchService) {
		this.watchService = watchService;
	}

	public static EtcdDiscoveryService create(EtcdWatchService watchService) {
		return new EtcdDiscoveryService(watchService);
	}

	public EtcdDiscoveryService withCrdtProvider(Function<PartitionId, CrdtStorage<?, ?>> crdtProvider) {
		this.crdtProvider = crdtProvider;
		return this;
	}

	public EtcdDiscoveryService withRpcProvider(Function<PartitionId, RpcStrategy> rpcProvider) {
		this.rpcProvider = rpcProvider;
		return this;
	}

	@Override
	public AsyncSupplier<PartitionScheme<PartitionId>> discover() {
		AsyncSupplier<byte[]> watchSupplier = watchService.watch();
		return () -> watchSupplier.get()
				.map(bytes -> {
					PartitionScheme<PartitionId> scheme;
					try {
						scheme = parseScheme(bytes);
					} catch (MalformedDataException e) {
						throw new CrdtException("Could not parse partition scheme", e);
					}
					return scheme;
				})
				.mapException(e -> !(e instanceof CrdtException),
						e -> new CrdtException("Failed to discover partitions", e));
	}

	private RendezvousPartitionScheme<PartitionId> parseScheme(byte[] bytes) throws MalformedDataException {
		List<RendezvousPartitionGroup<PartitionId>> partitionGroups = fromJson(PARTITION_GROUPS_TYPE, bytes);
		RendezvousPartitionScheme<PartitionId> scheme = RendezvousPartitionScheme.create(partitionGroups)
				.withPartitionIdGetter(PartitionId::getId);

		if (rpcProvider != null) scheme.withRpcProvider(rpcProvider);
		if (crdtProvider != null) scheme.withCrdtProvider(crdtProvider);

		return scheme;
	}
}
