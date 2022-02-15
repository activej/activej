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
import io.activej.crdt.storage.cluster.PartitionId;
import io.activej.crdt.storage.cluster.RendezvousPartitionGroup;
import io.activej.crdt.storage.cluster.RendezvousPartitionScheme;
import io.activej.rpc.client.sender.RpcStrategy;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static io.activej.common.Checks.checkArgument;
import static io.activej.config.converter.ConfigConverters.*;

public final class ConfigConverters {

	/**
	 * Config converter to create a {@link RendezvousPartitionScheme} out of a {@link Config}
	 * that is useful for creating {@link RpcStrategy} on a client side
	 *
	 * @return a config converter for {@link RendezvousPartitionScheme}
	 */
	public static <P> ConfigConverter<RendezvousPartitionScheme<P>> ofRendezvousPartitionScheme(
			@NotNull ConfigConverter<P> partitionIdConverter
	) {
		return new ConfigConverter<RendezvousPartitionScheme<P>>() {
			@Override
			public @NotNull RendezvousPartitionScheme<P> get(Config config) {
				Collection<Config> partitionGroupsConfig = config.getChild("partitionGroup").getChildren().values();

				List<RendezvousPartitionGroup<P>> partitionGroups = new ArrayList<>();
				for (Config partitionGroupConfig : partitionGroupsConfig) {
					partitionGroups.add(ofPartitionGroup(partitionIdConverter).get(partitionGroupConfig));
				}

				return RendezvousPartitionScheme.create(partitionGroups);
			}

			@Override
			@Contract("_, !null -> !null")
			public RendezvousPartitionScheme<P> get(Config config, @Nullable RendezvousPartitionScheme<P> defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				} else {
					return get(config);
				}
			}
		};
	}

	public static <P> ConfigConverter<RendezvousPartitionGroup<P>> ofPartitionGroup(ConfigConverter<P> partitionIdConverter) {
		return new ConfigConverter<RendezvousPartitionGroup<P>>() {
			@Override
			public @NotNull RendezvousPartitionGroup<P> get(Config config) {
				Set<P> ids = new HashSet<>(config.get(ofList(partitionIdConverter), "ids"));
				checkArgument(!ids.isEmpty(), "Empty partition ids");

				int replicas = config.get(ofInteger(), "replicas", 1);
				boolean repartition = config.get(ofBoolean(), "repartition", false);
				boolean active = config.get(ofBoolean(), "active", false);

				return RendezvousPartitionGroup.create(ids, replicas, repartition, active);
			}

			@Override
			@Contract("_, !null -> !null")
			public @Nullable RendezvousPartitionGroup<P> get(Config config, @Nullable RendezvousPartitionGroup<P> defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				} else {
					return get(config);
				}
			}
		};
	}

	public static ConfigConverter<PartitionId> ofPartitionId() {
		return SimpleConfigConverter.of(PartitionId::parseString, PartitionId::toString);
	}

}

