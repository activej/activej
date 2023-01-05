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

package io.activej.fs.cluster;

import io.activej.async.function.AsyncSupplier;
import io.activej.common.exception.MalformedDataException;
import io.activej.fs.AsyncFs;
import io.activej.fs.exception.FsException;
import io.activej.types.TypeT;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.activej.fs.util.JsonUtils.fromJson;

public final class EtcdDiscoveryService implements AsyncDiscoveryService {
	private final TypeT<Set<String>> PARTITION_IDS_TYPE_T = new TypeT<>() {};

	private final EtcdWatchService watchService;
	private final Function<String, AsyncFs> activeFsProvider;

	private EtcdDiscoveryService(EtcdWatchService watchService, Function<String, AsyncFs> activeFsProvider) {
		this.watchService = watchService;
		this.activeFsProvider = activeFsProvider;
	}

	public static EtcdDiscoveryService create(EtcdWatchService watchService, Function<String, AsyncFs> activeFsProvider) {
		return new EtcdDiscoveryService(watchService, activeFsProvider);
	}

	@Override
	public AsyncSupplier<Map<Object, AsyncFs>> discover() {
		AsyncSupplier<byte[]> watchSupplier = watchService.watch();
		return () -> watchSupplier.get()
				.map(bytes -> {
					Set<String> partitionIds;
					try {
						partitionIds = fromJson(PARTITION_IDS_TYPE_T, bytes);
					} catch (MalformedDataException e){
						throw new FsException("Could not parse partition ids: " + e.getMessage());
					}
					Map<Object, AsyncFs> result = new HashMap<>();
					for (String partitionId : partitionIds) {
						result.put(partitionId, activeFsProvider.apply(partitionId));
					}
					return result;
				})
				.mapException(e -> new FsException(e.getMessage()));
	}
}
