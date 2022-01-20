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
import io.activej.config.converter.ConfigConverter;
import io.activej.config.converter.SimpleConfigConverter;
import io.activej.crdt.storage.cluster.RendezvousPartitioning;
import io.activej.crdt.storage.cluster.RendezvousPartitionings;
import io.activej.crdt.storage.cluster.SimplePartitionId;
import io.activej.rpc.client.sender.RpcStrategy;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.ToIntFunction;

import static io.activej.common.Checks.checkArgument;
import static io.activej.config.converter.ConfigConverters.*;

public final class ConfigConverters {

	/**
	 * @see #ofRendezvousPartitionings(ConfigConverter, ToIntFunction)
	 */
	public static ConfigConverter<RendezvousPartitionings<SimplePartitionId>> ofRendezvousPartitionings() {
		return ofRendezvousPartitionings(Objects::hashCode);
	}

	/**
	 * @see #ofRendezvousPartitionings(ConfigConverter, ToIntFunction)
	 */
	public static <K extends Comparable<K>> ConfigConverter<RendezvousPartitionings<SimplePartitionId>> ofRendezvousPartitionings(
			@NotNull ToIntFunction<K> hashFn
	) {
		return ofRendezvousPartitionings(ofSimplePartitionId(), hashFn);
	}

	/**
	 * Config converter to create a {@link RendezvousPartitionings} out of a {@link Config}
	 * that is useful for creating {@link RpcStrategy} on a client side
	 *
	 * @return a config converter for {@link RendezvousPartitionings}
	 */
	public static <K extends Comparable<K>, P> ConfigConverter<RendezvousPartitionings<P>> ofRendezvousPartitionings(
			@NotNull ConfigConverter<P> partitionIdConverter,
			@NotNull ToIntFunction<K> hashFn
	) {
		return makeRendezvousPartitionings(partitionIdConverter, hashFn);
	}

	private static <K extends Comparable<K>, P> ConfigConverter<RendezvousPartitionings<P>> makeRendezvousPartitionings(
			@NotNull ConfigConverter<P> partitionIdConverter,
			@NotNull ToIntFunction<K> hashFn
	) {
		return new ConfigConverter<RendezvousPartitionings<P>>() {
			@Override
			public @NotNull RendezvousPartitionings<P> get(Config config) {
				Collection<Config> partitioningsConfig = config.getChild("partitionings").getChildren().values();

				List<RendezvousPartitioning<P>> partitionings = new ArrayList<>();
				for (Config partitioning : partitioningsConfig) {
					partitionings.add(ofPartitioning(partitionIdConverter).get(partitioning));
				}

				return RendezvousPartitionings.create(partitionings)
						.withHashFn(hashFn);
			}

			@Override
			@Contract("_, !null -> !null")
			public RendezvousPartitionings<P> get(Config config, @Nullable RendezvousPartitionings<P> defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				} else {
					return get(config);
				}
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

	public static ConfigConverter<SimplePartitionId> ofSimplePartitionId() {
		return SimpleConfigConverter.of(SimplePartitionId::parseString, SimplePartitionId::toString);
	}

}

