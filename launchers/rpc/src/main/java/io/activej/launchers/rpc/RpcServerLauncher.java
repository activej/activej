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

import io.activej.common.Initializer;
import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.di.annotation.Inject;
import io.activej.di.annotation.Optional;
import io.activej.di.annotation.Provides;
import io.activej.di.module.AbstractModule;
import io.activej.di.module.Module;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.ThrottlingController;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.promise.Promise;
import io.activej.rpc.server.RpcServer;
import io.activej.service.ServiceGraphModule;

import static io.activej.config.Config.ofClassPathProperties;
import static io.activej.config.Config.ofSystemProperties;
import static io.activej.di.module.Modules.combine;
import static io.activej.launchers.initializers.Initializers.ofEventloop;

public abstract class RpcServerLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "rpc-server.properties";
	public static final String BUSINESS_MODULE_PROP = "businessLogicModule";

	@Inject
	RpcServer rpcServer;

	@Provides
	public Eventloop eventloop(Config config,
			@Optional ThrottlingController throttlingController) {
		return Eventloop.create()
				.initialize(ofEventloop(config.getChild("eventloop")))
				.initialize(eventloop -> eventloop.withInspector(throttlingController));
	}

	@Provides
	Config config() {
		return Config.create()
				.overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(ofSystemProperties("config"));
	}

	@Override
	protected final Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				JmxModule.create(),
				ConfigModule.create()
						.withEffectiveConfigLogger(),
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
		String businessLogicModuleName = System.getProperty(BUSINESS_MODULE_PROP);

		Module businessLogicModule = businessLogicModuleName != null ?
				(Module) Class.forName(businessLogicModuleName).newInstance() :
				new AbstractModule() {
					@Provides
					Initializer<RpcServer> rpcServerInitializer() {
						return server -> server
								.withMessageTypes(String.class)
								.withHandler(String.class, String.class,
										req -> Promise.of("Request: " + req));
					}
				};

		Launcher launcher = new RpcServerLauncher() {
			@Override
			protected Module getBusinessLogicModule() {
				return businessLogicModule;
			}
		};

		launcher.launch(args);
	}
}
