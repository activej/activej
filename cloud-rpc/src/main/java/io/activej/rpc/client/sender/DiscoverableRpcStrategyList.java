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

import java.net.InetSocketAddress;
import java.util.*;

import static java.util.stream.Collectors.toList;

final class DiscoverableRpcStrategyList implements RpcStrategyList {
	private final DiscoveryService discoveryService;

	private RpcStrategyListFinal list;
	private Map<Object, InetSocketAddress> previouslyDiscovered = new HashMap<>();

	private DiscoverableRpcStrategyList(RpcStrategyListFinal list, DiscoveryService discoveryService) {
		this.list = list;
		this.discoveryService = discoveryService;
	}

	static DiscoverableRpcStrategyList create(DiscoveryService discoveryService) {
		RpcStrategyListFinal strategyList = new RpcStrategyListFinal(Collections.emptyList());
		DiscoverableRpcStrategyList list = new DiscoverableRpcStrategyList(strategyList, discoveryService);
		list.rediscover();
		return list;
	}

	private void rediscover() {
		discoveryService.discover(previouslyDiscovered,
				(result, e) -> {
					if (e == null) {
						this.previouslyDiscovered = result;
						this.list = toStrategyList(result.values());
						rediscover();
					} else {
						throw new RuntimeException("Failed to discover addresses", e);
					}
				});
	}

	@Override
	public List<RpcSender> listOfSenders(RpcClientConnectionPool pool) {
		return list.listOfSenders(pool);
	}

	@Override
	public List<RpcSender> listOfNullableSenders(RpcClientConnectionPool pool) {
		return list.listOfNullableSenders(pool);
	}

	@Override
	public DiscoveryService getDiscoveryService() {
		return discoveryService;
	}

	@Override
	public int size() {
		return list.size();
	}

	@Override
	public RpcStrategy get(int index) {
		return list.get(index);
	}

	private static RpcStrategyListFinal toStrategyList(Collection<InetSocketAddress> addresses) {
		return new RpcStrategyListFinal(addresses.stream()
				.map(RpcStrategySingleServer::create)
				.collect(toList()));
	}
}
