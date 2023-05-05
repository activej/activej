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

import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.client.IRpcClient;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.strategy.RpcStrategy;
import io.activej.rpc.protocol.RpcMessage;
import io.activej.serializer.SerializerFactory;

import java.net.InetSocketAddress;
import java.util.List;

import static io.activej.config.Config.ofClassPathProperties;
import static io.activej.config.Config.ofSystemProperties;
import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;
import static io.activej.launchers.crdt.rpc.CrdtRpcServerModule.DEFAULT_PORT;
import static io.activej.rpc.client.sender.strategy.RpcStrategies.server;

public class CrdtRpcClientModule extends AbstractModule {
	public static final String PROPERTIES_FILE = "crdt-rpc-client.properties";

	@Provides
	NioReactor reactor() {
		return Eventloop.create();
	}

	@Provides
	Config config() {
		return Config.create()
				.with("address", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(DEFAULT_PORT)))
				.overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(ofSystemProperties("config"));
	}

	@Provides
	IRpcClient client(NioReactor reactor, RpcStrategy strategy, List<Class<?>> messageTypes) {
		return RpcClient.builder(reactor)
				.withSerializer(SerializerFactory.builder()
						.withSubclasses(RpcMessage.SUBCLASSES_ID, messageTypes)
						.build()
						.create(RpcMessage.class))
				.withStrategy(strategy)
				.build();
	}

	@Provides
	RpcStrategy strategy(Config config) {
		return server(config.get(ofInetSocketAddress(), "address"));
	}
}
