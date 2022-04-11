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
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.inject.BinarySerializerModule.BinarySerializerLocator;
import io.activej.dataflow.inject.DataflowModule;
import io.activej.dataflow.inject.SortingExecutor;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowRequest;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowResponse;
import io.activej.dataflow.proto.FunctionSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.inspector.ThrottlingController;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.Module;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.service.ServiceGraphModule;

import java.util.concurrent.Executor;

import static io.activej.config.converter.ConfigConverters.getExecutor;
import static io.activej.config.converter.ConfigConverters.ofPath;
import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.initializers.Initializers.ofAbstractServer;
import static io.activej.launchers.initializers.Initializers.ofEventloop;

public abstract class DataflowServerLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "dataflow-server.properties";

	@Inject
	DataflowServer dataflowServer;

	@Provides
	Eventloop eventloop(Config config, OptionalDependency<ThrottlingController> throttlingController) {
		return Eventloop.create()
				.withInitializer(ofEventloop(config.getChild("eventloop")))
				.withInitializer(eventloop -> eventloop.withInspector(throttlingController.orElse(null)));
	}

	@Provides
	Executor executor(Config config) {
		return getExecutor(config);
	}

	@Provides
	@Eager
	@SortingExecutor
	Executor sortingExecutor(Config config) {
		return getExecutor(config.getChild("sortingExecutor"));
	}

	@Provides
	DataflowServer server(Eventloop eventloop, Config config, ByteBufsCodec<DataflowRequest, DataflowResponse> codec, BinarySerializerLocator serializers, Injector environment, FunctionSerializer functionSerializer) {
		return DataflowServer.create(eventloop, codec, serializers, environment, functionSerializer)
				.withInitializer(ofAbstractServer(config.getChild("dataflow.server")))
				.withInitializer(s -> s.withSocketSettings(s.getSocketSettings().withTcpNoDelay(true)));
	}

	@Provides
	@Eager
	DataflowClient client(Executor executor, Config config, ByteBufsCodec<DataflowResponse, DataflowRequest> codec, BinarySerializerLocator serializers, FunctionSerializer functionSerializer) {
		return new DataflowClient(executor, config.get(ofPath(), "dataflow.secondaryBufferPath"), codec, serializers, functionSerializer);
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
}
