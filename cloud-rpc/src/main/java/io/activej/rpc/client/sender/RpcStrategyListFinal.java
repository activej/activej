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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

class RpcStrategyListFinal implements RpcStrategyList {
	private final List<? extends RpcStrategy> strategies;

	RpcStrategyListFinal(List<? extends RpcStrategy> strategies) {
		this.strategies = strategies;
	}

	@Override
	public List<RpcSender> listOfSenders(RpcClientConnectionPool pool) {
		return strategies.stream()
				.map(strategy -> strategy.createSender(pool))
				.filter(Objects::nonNull)
				.collect(toList());
	}

	@Override
	public List<RpcSender> listOfNullableSenders(RpcClientConnectionPool pool) {
		return strategies.stream()
				.map(strategy -> strategy.createSender(pool))
				.collect(toList());
	}

	@Override
	public DiscoveryService getDiscoveryService() {
		return DiscoveryService.combined(strategies.stream()
				.map(RpcStrategy::getDiscoveryService)
				.collect(Collectors.toList()));
	}

	@Override
	public int size() {
		return strategies.size();
	}

	@Override
	public RpcStrategy get(int index) {
		return strategies.get(index);
	}

	public List<RpcStrategy> getStrategies() {
		return Collections.unmodifiableList(strategies);
	}
}
