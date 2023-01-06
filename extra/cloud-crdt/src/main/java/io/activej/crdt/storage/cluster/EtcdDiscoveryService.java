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
import io.activej.fs.cluster.EtcdWatchService;
import io.activej.reactor.Reactor;

public final class EtcdDiscoveryService extends AbstractDiscoveryService<EtcdDiscoveryService> {
	private final EtcdWatchService watchService;

	private EtcdDiscoveryService(Reactor reactor, EtcdWatchService watchService) {
		super(reactor);
		this.watchService = watchService;
	}

	public static EtcdDiscoveryService create(Reactor reactor, EtcdWatchService watchService) {
		return new EtcdDiscoveryService(reactor, watchService);
	}

	@Override
	public AsyncSupplier<PartitionScheme<PartitionId>> discover() {
		checkInReactorThread();
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
}
