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

package io.activej.launchers.dataflow;

import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.dataflow.di.BinarySerializerModule.BinarySerializerLocator;
import io.activej.dataflow.di.DataflowModule;
import io.activej.dataflow.server.DataflowClient;
import io.activej.dataflow.server.DataflowServer;
import io.activej.dataflow.server.command.DatagraphCommand;
import io.activej.dataflow.server.command.DatagraphResponse;
import io.activej.di.Injector;
import io.activej.di.annotation.Inject;
import io.activej.di.annotation.Optional;
import io.activej.di.annotation.Provides;
import io.activej.di.module.Module;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.ThrottlingController;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.service.ServiceGraphModule;

import java.util.concurrent.Executor;

import static io.activej.config.ConfigConverters.getExecutor;
import static io.activej.config.ConfigConverters.ofPath;
import static io.activej.di.module.Modules.combine;
import static io.activej.launchers.initializers.Initializers.ofAbstractServer;
import static io.activej.launchers.initializers.Initializers.ofEventloop;

public abstract class DataflowServerLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "dataflow-server.properties";
	public static final String BUSINESS_MODULE_PROP = "businessLogicModule";

	@Inject
	DataflowServer dataflowServer;

	@Provides
	Eventloop eventloop(Config config, @Optional ThrottlingController throttlingController) {
		return Eventloop.create()
				.initialize(ofEventloop(config.getChild("eventloop")))
				.initialize(eventloop -> eventloop.withInspector(throttlingController));
	}

	@Provides
	Executor executor(Config config) {
		return getExecutor(config);
	}

	@Provides
	DataflowServer server(Eventloop eventloop, Config config, ByteBufsCodec<DatagraphCommand, DatagraphResponse> codec, BinarySerializerLocator serializers, Injector environment) {
		return new DataflowServer(eventloop, codec, serializers, environment)
				.initialize(ofAbstractServer(config.getChild("dataflow.server")))
				.initialize(s -> s.withSocketSettings(s.getSocketSettings().withTcpNoDelay(true)));
	}

	@Provides
	DataflowClient client(Executor executor, Config config, ByteBufsCodec<DatagraphResponse, DatagraphCommand> codec, BinarySerializerLocator serializers) {
		return new DataflowClient(executor, config.get(ofPath(), "dataflow.secondaryBufferPath"), codec, serializers);
	}

	@Provides
	Config config() {
		return Config.create()
				.overrideWith(Config.ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
	}

	@Override
	protected final Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				JmxModule.create(),
				DataflowModule.create(),
				ConfigModule.create()
						.withEffectiveConfigLogger(),
				getBusinessLogicModule()
		);
	}

	/**
	 * Override this method to supply your launcher business logic.
	 */
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
				Module.empty();

		Launcher launcher = new DataflowServerLauncher() {
			@Override
			protected Module getBusinessLogicModule() {
				return businessLogicModule;
			}
		};

		launcher.launch(args);
	}
}
