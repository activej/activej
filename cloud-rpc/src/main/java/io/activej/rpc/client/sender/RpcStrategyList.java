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
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.util.*;

import static io.activej.common.Checks.checkArgument;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public final class RpcStrategyList {
	private final List<RpcStrategy> strategies;

	private RpcStrategyList(List<RpcStrategy> strategies) {
		this.strategies = strategies;
	}

	public static RpcStrategyList ofAddresses(@NotNull List<InetSocketAddress> addresses) {
		checkArgument(!addresses.isEmpty(), "At least one address must be present");
		return new RpcStrategyList(addresses.stream()
				.map(RpcStrategySingleServer::create)
				.collect(toList()));
	}

	public static RpcStrategyList ofStrategies(List<RpcStrategy> strategies) {
		return new RpcStrategyList(new ArrayList<>(strategies));
	}

	public List<RpcSender> listOfSenders(RpcClientConnectionPool pool) {
		return strategies.stream()
				.map(strategy -> strategy.createSender(pool))
				.filter(Objects::nonNull)
				.collect(toList());
	}

	public List<RpcSender> listOfNullableSenders(RpcClientConnectionPool pool) {
		return strategies.stream()
				.map(strategy -> strategy.createSender(pool))
				.collect(toList());
	}

	public Set<InetSocketAddress> getAddresses() {
		return strategies.stream()
				.map(RpcStrategy::getAddresses)
				.flatMap(Collection::stream)
				.collect(toSet());
	}

	public int size() {
		return strategies.size();
	}

	public RpcStrategy get(int index) {
		return strategies.get(index);
	}

}
