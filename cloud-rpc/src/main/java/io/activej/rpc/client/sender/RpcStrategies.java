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

import io.activej.rpc.hash.HashFunction;
import io.activej.rpc.hash.ShardingFunction;
import io.activej.rpc.protocol.RpcException;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.util.List;

import static io.activej.common.Checks.checkArgument;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

/**
 * Strategies used in RPC can be divided in three following categories:
 * <ul>
 *     <li>Getting a single RPC-service</li>
 *     <li>Failover</li>
 *     <li>Load balancing</li>
 *     <li>Rendezvous hashing</li>
 * </ul>
 */
public final class RpcStrategies {
	static final RpcException NO_SENDER_AVAILABLE_EXCEPTION = new RpcException("No senders available");

	public static RpcStrategySingleServer server(@NotNull InetSocketAddress address) {
		return RpcStrategySingleServer.create(address);
	}

	public static List<RpcStrategy> servers(InetSocketAddress... addresses) {
		return servers(asList(addresses));
	}

	public static List<RpcStrategy> servers(List<InetSocketAddress> addresses) {
		checkArgument(!addresses.isEmpty(), "At least one address must be present");
		return addresses.stream()
				.map(RpcStrategySingleServer::create)
				.collect(toList());
	}

	public static RpcStrategyFirstAvailable firstAvailable(RpcStrategy... senders) {
		return RpcStrategyFirstAvailable.create(senders);
	}

	public static RpcStrategyFirstAvailable firstAvailable(List<RpcStrategy> list) {
		return RpcStrategyFirstAvailable.create(list);
	}

	public static RpcStrategyFirstValidResult firstValidResult(RpcStrategy... senders) {
		return RpcStrategyFirstValidResult.create(senders);
	}

	public static RpcStrategyFirstValidResult firstValidResult(List<RpcStrategy> list) {
		return RpcStrategyFirstValidResult.create(list);
	}

	public static RpcStrategyRoundRobin roundRobin(RpcStrategy... senders) {
		return RpcStrategyRoundRobin.create(senders);
	}

	public static RpcStrategyRoundRobin roundRobin(List<RpcStrategy> list) {
		return RpcStrategyRoundRobin.create(list);
	}

	public static RpcStrategySharding sharding(ShardingFunction<?> hashFunction, RpcStrategy... senders) {
		return RpcStrategySharding.create(hashFunction, senders);
	}

	public static RpcStrategySharding sharding(@NotNull ShardingFunction<?> hashFunction, @NotNull List<RpcStrategy> list) {
		return RpcStrategySharding.create(hashFunction, list);
	}

	public static RpcStrategyRendezvousHashing rendezvousHashing(@NotNull HashFunction<?> hashFunction) {
		return RpcStrategyRendezvousHashing.create(hashFunction);
	}

	public static RpcStrategyTypeDispatching typeDispatching() {
		return RpcStrategyTypeDispatching.create();
	}

	public static RpcStrategyRandomSampling randomSampling() {
		return RpcStrategyRandomSampling.create();
	}
}

