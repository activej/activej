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

package io.activej.launchers.crdt.rpc;

import io.activej.async.function.AsyncSupplier;
import io.activej.async.service.ReactiveService;
import io.activej.crdt.storage.cluster.AsyncDiscoveryService;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.rpc.client.RpcClient_Reactive;
import io.activej.rpc.client.sender.RpcStrategy;

import java.util.function.Function;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.Checks.checkState;

public final class CrdtRpcStrategyService<K extends Comparable<K>> extends AbstractReactive
		implements ReactiveService {
	private final AsyncDiscoveryService<?> discoveryService;
	private final Function<Object, K> keyGetter;

	private RpcClient_Reactive rpcClient;
	private Function<RpcStrategy, RpcStrategy> strategyMapFn = Function.identity();

	private boolean stopped;

	private CrdtRpcStrategyService(Reactor reactor, AsyncDiscoveryService<?> discoveryService, Function<Object, K> keyGetter) {
		super(reactor);
		this.discoveryService = discoveryService;
		this.keyGetter = keyGetter;
	}

	public static <K extends Comparable<K>> CrdtRpcStrategyService<K> create(Reactor reactor, AsyncDiscoveryService<?> discoveryService, Function<Object, K> keyGetter) {
		return new CrdtRpcStrategyService<>(reactor, discoveryService, keyGetter);
	}

	public CrdtRpcStrategyService<K> withStrategyMapping(Function<RpcStrategy, RpcStrategy> strategyMapFn) {
		this.strategyMapFn = strategyMapFn;
		return this;
	}

	public void setRpcClient(RpcClient_Reactive rpcClient) {
		checkState(this.rpcClient == null && rpcClient.getReactor() == reactor);

		this.rpcClient = rpcClient;
	}

	@Override
	public Promise<?> start() {
		checkInReactorThread();
		checkNotNull(rpcClient);

		AsyncSupplier<? extends AsyncDiscoveryService.PartitionScheme<?>> discoverySupplier = discoveryService.discover();
		return discoverySupplier.get()
				.whenResult(partitionScheme -> {
					RpcStrategy rpcStrategy = partitionScheme.createRpcStrategy(keyGetter);
					rpcClient.withStrategy(strategyMapFn.apply(rpcStrategy));
					Promises.repeat(() ->
							discoverySupplier.get()
									.map((newPartitionScheme, e) -> {
										if (stopped) return false;
										if (e == null) {
											RpcStrategy newRpcStrategy = newPartitionScheme.createRpcStrategy(keyGetter);
											rpcClient.changeStrategy(strategyMapFn.apply(newRpcStrategy), true);
										}
										return true;
									})
					);
				});

	}

	@Override
	public Promise<?> stop() {
		checkInReactorThread();
		this.stopped = true;
		return Promise.complete();
	}
}
