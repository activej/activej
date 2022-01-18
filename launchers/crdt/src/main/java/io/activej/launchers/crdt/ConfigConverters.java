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

import io.activej.common.exception.MalformedDataException;
import io.activej.config.Config;
import io.activej.config.converter.ConfigConverter;
import io.activej.config.converter.SimpleConfigConverter;
import io.activej.crdt.CrdtStorageClient;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.cluster.DiscoveryService;
import io.activej.crdt.storage.cluster.RendezvousPartitioning;
import io.activej.crdt.storage.cluster.RendezvousPartitionings;
import io.activej.crdt.storage.cluster.SimplePartitionId;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.rpc.client.sender.RpcStrategy;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.function.ToLongBiFunction;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.difference;
import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.crdt.storage.cluster.RendezvousPartitionings.defaultHashBucketFn;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public final class ConfigConverters {

	public static ConfigConverter<SimplePartitionId> ofSimplePartitionId() {
		return new SimpleConfigConverter<SimplePartitionId>() {
			@Override
			protected SimplePartitionId fromString(String string) {
				try {
					return SimplePartitionId.parseString(string);
				} catch (MalformedDataException e) {
					throw new IllegalArgumentException(e);
				}
			}

			@Override
			protected String toString(SimplePartitionId simplePartitionId) {
				return simplePartitionId.asString();
			}
		};
	}

	public static <P> ConfigConverter<RendezvousPartitioning<P>> ofPartitioning(ConfigConverter<P> partitionIdConverter) {
		return new ConfigConverter<RendezvousPartitioning<P>>() {
			@Override
			public @NotNull RendezvousPartitioning<P> get(Config config) {
				Set<P> ids = new HashSet<>(config.get(ofList(partitionIdConverter), "ids"));
				checkArgument(!ids.isEmpty(), "Empty partitioning ids");

				int replicas = config.get(ofInteger(), "replicas", 1);
				boolean repartition = config.get(ofBoolean(), "repartition", false);
				boolean active = config.get(ofBoolean(), "active", false);

				return RendezvousPartitioning.create(ids, replicas, repartition, active);
			}

			@Override
			@Contract("_, !null -> !null")
			public @Nullable RendezvousPartitioning<P> get(Config config, @Nullable RendezvousPartitioning<P> defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				} else {
					return get(config);
				}
			}
		};
	}

	public static <K extends Comparable<K>, S, P> ConfigConverter<DiscoveryService<K, S, P>> ofDiscoveryService(
			@NotNull ConfigConverter<P> partitionIdConverter,
			@NotNull Function<P, CrdtStorage<K, S>> storageFactory,
			@NotNull ToIntFunction<K> hashFn,
			@NotNull ToLongBiFunction<P, Integer> hashBucketFn
	) {
		return createOfDiscoveryService(partitionIdConverter, hashFn, hashBucketFn, storageFactory);
	}

	public static <K extends Comparable<K>, S> ConfigConverter<DiscoveryService<K, S, SimplePartitionId>> ofDiscoveryService(
			@NotNull Eventloop eventloop,
			@NotNull CrdtDataSerializer<K, S> serializer,
			@NotNull SimplePartitionId localId,
			@NotNull CrdtStorage<K, S> localStorage,
			@NotNull ToIntFunction<K> hashFn
	) {
		return createOfDiscoveryService(ofSimplePartitionId(), hashFn, defaultHashBucketFn(value -> value.getId().hashCode()),
				partitionId ->
						localId.equals(partitionId) ?
								localStorage :
								CrdtStorageClient.create(eventloop, partitionId.getCrdtAddress(), serializer));
	}

	public static <K extends Comparable<K>, S> ConfigConverter<DiscoveryService<K, S, SimplePartitionId>> ofDiscoveryService(
			@NotNull Eventloop eventloop,
			@NotNull CrdtDataSerializer<K, S> serializer,
			@NotNull SimplePartitionId localId,
			@NotNull CrdtStorage<K, S> localStorage
	) {
		return ofDiscoveryService(eventloop, serializer, localId, localStorage, Objects::hashCode);
	}

	/**
	 * Config converter to create a {@link DiscoveryService} out of a {@link Config}
	 * that is useful for creating {@link RpcStrategy} on a client side
	 *
	 * @return a config converter for {@link DiscoveryService}
	 */
	public static <K extends Comparable<K>, S, P> ConfigConverter<DiscoveryService<K, S, P>> ofDiscoveryService(
			@NotNull ConfigConverter<P> partitionIdConverter,
			@NotNull ToIntFunction<K> hashFn,
			@NotNull ToLongBiFunction<P, Integer> hashBucketFn
	) {
		return createOfDiscoveryService(partitionIdConverter, hashFn, hashBucketFn, null);
	}

	/**
	 * @see #ofDiscoveryService(ConfigConverter, ToIntFunction, ToLongBiFunction)
	 */
	public static <K extends Comparable<K>, S> ConfigConverter<DiscoveryService<K, S, SimplePartitionId>> ofDiscoveryService(
			@NotNull ToIntFunction<K> hashFn
	) {
		return ofDiscoveryService(ofSimplePartitionId(), hashFn,
				defaultHashBucketFn(partitionId -> partitionId.getId().hashCode()));
	}

	/**
	 * @see #ofDiscoveryService(ConfigConverter, ToIntFunction, ToLongBiFunction)
	 */
	public static <K extends Comparable<K>, S> ConfigConverter<DiscoveryService<K, S, SimplePartitionId>> ofDiscoveryService() {
		return ofDiscoveryService(Objects::hashCode);
	}

	private static <S, K extends Comparable<K>, P> ConfigConverter<DiscoveryService<K, S, P>> createOfDiscoveryService(
			@NotNull ConfigConverter<P> partitionIdConverter,
			@NotNull ToIntFunction<K> hashFn,
			@NotNull ToLongBiFunction<P, Integer> hashBucketFn,
			@Nullable Function<P, CrdtStorage<K, S>> storageFactory
	) {
		return new ConfigConverter<DiscoveryService<K, S, P>>() {
			@Override
			public @NotNull DiscoveryService<K, S, P> get(Config config) {
				Collection<Config> partitioningsConfig = config.getChild("partitionings").getChildren().values();

				List<RendezvousPartitioning<P>> partitionings = new ArrayList<>();
				for (Config partitioning : partitioningsConfig) {
					partitionings.add(ofPartitioning(partitionIdConverter).get(partitioning));
				}

				if (storageFactory == null) {
					// Discovery service for RPC strategies
					return toDiscoveryService(partitionings, emptyMap(), hashFn, hashBucketFn);
				}

				Set<P> allIds = partitionings.stream()
						.map(RendezvousPartitioning::getPartitions)
						.flatMap(Collection::stream)
						.collect(toSet());

				Map<P, CrdtStorage<K, S>> storages = allIds.stream()
						.collect(toMap(Function.identity(), storageFactory));

				Set<P> missingAddresses = difference(allIds, storages.keySet());
				checkArgument(missingAddresses.isEmpty(), "There are partitions with missing addresses: " + missingAddresses);

				Set<P> danglingAddresses = difference(storages.keySet(), allIds);
				checkArgument(danglingAddresses.isEmpty(), "There are dangling addresses: " + danglingAddresses);

				return toDiscoveryService(partitionings, storages, hashFn, hashBucketFn);
			}

			@Override
			@Contract("_, !null -> !null")
			public @Nullable DiscoveryService<K, S, P> get(Config config, @Nullable DiscoveryService<K, S, P> defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				} else {
					return get(config);
				}
			}
		};
	}

	private static <K extends Comparable<K>, S, P> DiscoveryService<K, S, P> toDiscoveryService(
			List<RendezvousPartitioning<P>> partitionings,
			Map<P, CrdtStorage<K, S>> partitions,
			ToIntFunction<K> hashFn,
			ToLongBiFunction<P, Integer> hashBucketFn
	) {
		RendezvousPartitionings<K, S, P> rendezvousPartitionings = RendezvousPartitionings.create(partitions)
				.withHashBucketFn(hashBucketFn)
				.withHashFn(hashFn);
		for (RendezvousPartitioning<P> partitioning : partitionings) {
			rendezvousPartitionings = rendezvousPartitionings.withPartitioning(partitioning);
		}

		return DiscoveryService.of(rendezvousPartitionings);
	}


}

