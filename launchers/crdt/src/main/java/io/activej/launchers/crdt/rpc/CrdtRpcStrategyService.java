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
import io.activej.common.builder.AbstractBuilder;
import io.activej.crdt.storage.cluster.IDiscoveryService;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.strategy.RpcStrategy;

import java.util.function.Function;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.Checks.checkState;
import static io.activej.reactor.Reactive.checkInReactorThread;

public final class CrdtRpcStrategyService<K extends Comparable<K>> extends AbstractReactive
	implements ReactiveService {
	private final IDiscoveryService<?> discoveryService;
	private final Function<Object, K> keyGetter;

	private RpcClient rpcClient;
	private Function<RpcStrategy, RpcStrategy> strategyMapFn = Function.identity();

	private boolean stopped;

	private CrdtRpcStrategyService(Reactor reactor, IDiscoveryService<?> discoveryService, Function<Object, K> keyGetter) {
		super(reactor);
		this.discoveryService = discoveryService;
		this.keyGetter = keyGetter;
	}

	public static <K extends Comparable<K>> CrdtRpcStrategyService<K> create(Reactor reactor, IDiscoveryService<?> discoveryService, Function<Object, K> keyGetter) {
		return builder(reactor, discoveryService, keyGetter).build();
	}

	public static <K extends Comparable<K>> CrdtRpcStrategyService<K>.Builder builder(Reactor reactor, IDiscoveryService<?> discoveryService, Function<Object, K> keyGetter) {
		return new CrdtRpcStrategyService<>(reactor, discoveryService, keyGetter).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, CrdtRpcStrategyService<K>> {
		private Builder() {}

		public Builder withStrategyMapping(Function<RpcStrategy, RpcStrategy> strategyMapFn) {
			checkNotBuilt(this);
			CrdtRpcStrategyService.this.strategyMapFn = strategyMapFn;
			return this;
		}

		@Override
		protected CrdtRpcStrategyService<K> doBuild() {
			return CrdtRpcStrategyService.this;
		}
	}

	public void setRpcClient(RpcClient rpcClient) {
		checkState(this.rpcClient == null);
		this.rpcClient = rpcClient;
	}

	@Override
	public Promise<?> start() {
		checkInReactorThread(this);
		checkNotNull(rpcClient);

		AsyncSupplier<? extends IDiscoveryService.PartitionScheme<?>> discoverySupplier = discoveryService.discover();
		return discoverySupplier.get()
			.whenResult(partitionScheme -> {
				RpcStrategy rpcStrategy = partitionScheme.createRpcStrategy(keyGetter);
				rpcClient.changeStrategy(strategyMapFn.apply(rpcStrategy), false);
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
		checkInReactorThread(this);
		this.stopped = true;
		return Promise.complete();
	}
}
