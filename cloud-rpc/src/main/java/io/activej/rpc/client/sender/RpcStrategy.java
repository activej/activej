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
import java.util.List;
import java.util.Set;
import java.util.function.ToIntFunction;

import static io.activej.common.Checks.checkArgument;
import static io.activej.rpc.client.sender.RpcStrategy_FirstValidResult.DEFAULT_RESULT_VALIDATOR;
import static java.util.stream.Collectors.toList;

public interface RpcStrategy {
	Set<InetSocketAddress> getAddresses();

	@Nullable RpcSender createSender(RpcClientConnectionPool pool);

	static RpcStrategy server(InetSocketAddress address) {
		return new RpcStrategy_SingleServer(address);
	}

	static List<RpcStrategy> servers(InetSocketAddress... addresses) {
		return servers(List.of(addresses));
	}

	static List<RpcStrategy> servers(List<InetSocketAddress> addresses) {
		checkArgument(!addresses.isEmpty(), "At least one address must be present");
		return addresses.stream()
				.map(RpcStrategy_SingleServer::new)
				.collect(toList());
	}

	static RpcStrategy firstAvailable(RpcStrategy... strategies) {
		return firstAvailable(List.of(strategies));
	}

	static RpcStrategy firstAvailable(List<RpcStrategy> strategies) {
		return new RpcStrategy_FirstAvailable(strategies);
	}

	static RpcStrategy_FirstValidResult firstValidResult(RpcStrategy... strategies) {
		return firstValidResult(List.of(strategies));
	}

	static RpcStrategy_FirstValidResult firstValidResult(List<RpcStrategy> strategies) {
		return new RpcStrategy_FirstValidResult(strategies, DEFAULT_RESULT_VALIDATOR, null);
	}

	static RpcStrategy_RandomSampling randomSampling() {
		return new RpcStrategy_RandomSampling();
	}

	static <T> RpcStrategy_RendezvousHashing rendezvousHashing(ToIntFunction<T> hashFunction) {
		return new RpcStrategy_RendezvousHashing(hashFunction);
	}

	static RpcStrategy_RoundRobin roundRobin(RpcStrategy... strategies) {
		return roundRobin(List.of(strategies));
	}

	static RpcStrategy_RoundRobin roundRobin(List<RpcStrategy> strategies) {
		return new RpcStrategy_RoundRobin(strategies, 0);
	}

	static <T> RpcStrategy_Sharding sharding(ToIntFunction<T> shardingFunction, RpcStrategy... strategies) {
		return sharding(shardingFunction, List.of(strategies));
	}

	static <T> RpcStrategy_Sharding sharding(ToIntFunction<T> shardingFunction, List<RpcStrategy> strategies) {
		return new RpcStrategy_Sharding(shardingFunction, strategies, 0);
	}

	static RpcStrategy_TypeDispatching typeDispatching() {
		return new RpcStrategy_TypeDispatching();
	}

	@FunctionalInterface
	interface ResultValidator<T> {
		boolean isValidResult(T value);
	}
}
