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

package io.activej.rpc.client.sender;

import io.activej.rpc.client.RpcClientConnectionPool;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.stream.Collectors;

import static io.activej.common.Checks.checkArgument;

final class DiscoverableRpcStrategyRendezvousHashing implements RpcStrategy {
	private final DiscoveryService discoveryService;

	private final RpcStrategyRendezvousHashing rendezvousHashing;

	private DiscoverableRpcStrategyRendezvousHashing(RpcStrategyRendezvousHashing rendezvousHashing, DiscoveryService discoveryService) {
		this.rendezvousHashing = rendezvousHashing;
		this.discoveryService = discoveryService;
	}

	static DiscoverableRpcStrategyRendezvousHashing create(DiscoveryService discoveryService, RpcStrategyRendezvousHashing rendezvousHashing) {
		checkArgument(rendezvousHashing.getShards().isEmpty(), "Rendezvous hashing strategy should not contain any partition");

		DiscoverableRpcStrategyRendezvousHashing list = new DiscoverableRpcStrategyRendezvousHashing(rendezvousHashing, discoveryService);
		list.rediscover();
		return list;
	}

	private void rediscover() {
		Map<Object, InetSocketAddress> previouslyDiscovered = rendezvousHashing.getShards()
				.entrySet()
				.stream()
				.collect(Collectors.toMap(Map.Entry::getKey, e -> ((RpcStrategySingleServer) e.getValue()).getAddress()));

		discoveryService.discover(previouslyDiscovered,
				(result, e) -> {
					if (e == null) {
						Map<Object, RpcStrategy> newStrategies = result.entrySet().stream()
								.collect(Collectors.toMap(Map.Entry::getKey, entry -> RpcStrategySingleServer.create(entry.getValue())));

						this.rendezvousHashing.setShards(newStrategies);
						rediscover();
					} else {
						throw new RuntimeException("Failed to discover addresses", e);
					}
				});
	}

	@Override
	public DiscoveryService getDiscoveryService() {
		return discoveryService;
	}

	@Override
	public @Nullable RpcSender createSender(RpcClientConnectionPool pool) {
		return rendezvousHashing.createSender(pool);
	}
}
