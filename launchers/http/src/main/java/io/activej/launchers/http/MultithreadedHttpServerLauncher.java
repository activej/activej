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

package io.activej.launchers.http;

import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.inspector.ThrottlingController;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.HttpServer;
import io.activej.http.HttpUtils;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.net.PrimaryServer;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;

import java.net.InetSocketAddress;
import java.util.stream.Stream;

import static io.activej.config.Config.ofClassPathProperties;
import static io.activej.config.Config.ofSystemProperties;
import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;
import static io.activej.config.converter.ConfigConverters.ofInteger;
import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.initializers.Initializers.*;
import static java.util.stream.Collectors.joining;

@SuppressWarnings("WeakerAccess")
public abstract class MultithreadedHttpServerLauncher extends Launcher {
	public static final String HOSTNAME = "localhost";
	public static final int PORT = 8080;
	public static final int WORKERS = 4;

	public static final String PROPERTIES_FILE = "http-server.properties";

	@Inject
	PrimaryServer primaryServer;

	@Provides
	NioReactor primaryReactor(Config config) {
		return Eventloop.builder()
				.initialize(ofEventloop(config.getChild("eventloop.primary")))
				.build();
	}

	@Provides
	@Worker
	NioReactor workerReactor(Config config, OptionalDependency<ThrottlingController> throttlingController) {
		return Eventloop.builder()
				.initialize(ofEventloop(config.getChild("eventloop.worker")))
				.withInspector(throttlingController.orElse(null))
				.build();
	}

	@Provides
	WorkerPool workerPool(WorkerPools workerPools, Config config) {
		return workerPools.createPool(config.get(ofInteger(), "workers", WORKERS));
	}

	@Provides
	PrimaryServer primaryServer(NioReactor primaryReactor, WorkerPool.Instances<HttpServer> workerServers, Config config) {
		return PrimaryServer.builder(primaryReactor, workerServers.getList())
				.initialize(ofPrimaryServer(config.getChild("http")))
				.build();
	}

	@Provides
	@Worker
	HttpServer workerServer(NioReactor reactor, AsyncServlet servlet, Config config) {
		return HttpServer.builder(reactor, servlet)
				.initialize(ofHttpWorker(config.getChild("http")))
				.build();
	}

	@Provides
	Config config() {
		return Config.create()
				.with("http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(HOSTNAME, PORT)))
				.with("workers", "" + WORKERS)
				.overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(ofSystemProperties("config"));
	}

	@Override
	protected final Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				WorkerPoolModule.create(),
				ConfigModule.builder()
						.withEffectiveConfigLogger()
						.build(),
				getBusinessLogicModule()
		);
	}

	protected Module getBusinessLogicModule() {
		return Module.empty();
	}

	@Override
	protected void run() throws Exception {
		if (logger.isInfoEnabled()) {
			logger.info("HTTP Server is listening on {}", Stream.concat(
							primaryServer.getBoundAddresses().stream().map(address -> HttpUtils.formatUrl(address, false)),
							primaryServer.getSslBoundAddresses().stream().map(address -> HttpUtils.formatUrl(address, true)))
					.collect(joining(" ")));
		}
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {

		Launcher launcher = new MultithreadedHttpServerLauncher() {
			@Override
			protected Module getBusinessLogicModule() {
				return new AbstractModule() {
					@Provides
					@Worker
					AsyncServlet servlet(@WorkerId int workerId) {
						return request -> HttpResponse.Builder.ok200()
								.withPlainText("Hello, world! #" + workerId)
								.toPromise();
					}
				};
			}
		};

		launcher.launch(args);
	}
}
