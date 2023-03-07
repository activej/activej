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

package io.activej.launchers.rpc;

import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.inspector.ThrottlingController;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.promise.Promise;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.protocol.RpcMessageSerializer;
import io.activej.rpc.server.RpcServer;
import io.activej.service.ServiceGraphModule;

import java.net.InetSocketAddress;

import static io.activej.config.Config.ofClassPathProperties;
import static io.activej.config.Config.ofSystemProperties;
import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;
import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.initializers.Initializers.ofEventloop;

public abstract class RpcServerLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "rpc-server.properties";

	@Inject
	RpcServer rpcServer;

	@Provides
	public NioReactor reactor(Config config,
			OptionalDependency<ThrottlingController> throttlingController) {
		return Eventloop.builder()
				.initialize(ofEventloop(config.getChild("eventloop")))
				.withInspector(throttlingController.orElse(null))
				.build();
	}

	@Provides
	Config config() {
		return Config.create()
				.with("listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(9000)))
				.overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(ofSystemProperties("config"));
	}

	@Override
	protected final Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				JmxModule.create(),
				ConfigModule.builder()
						.withEffectiveConfigLogger()
						.build(),
				getBusinessLogicModule());
	}

	// By design, user should provide rpcServer here.
	protected Module getBusinessLogicModule() {
		return Module.empty();
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new RpcServerLauncher() {
			@Override
			protected Module getBusinessLogicModule() {
				return new AbstractModule() {
					@Provides
					RpcServer server(NioReactor reactor, Config config) {
						return RpcServer.builder(reactor)
								.withListenAddress(config.get(ofInetSocketAddress(), "listenAddresses"))
								.withSerializer(RpcMessageSerializer.of(String.class))
								.withHandler(String.class,
										req -> Promise.of("Request: " + req))
								.build();
					}
				};
			}
		}.launch(args);
	}
}
