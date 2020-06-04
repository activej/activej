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
import io.activej.di.annotation.Inject;
import io.activej.di.annotation.Optional;
import io.activej.di.annotation.Provides;
import io.activej.di.module.AbstractModule;
import io.activej.di.module.Module;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.inspector.ThrottlingController;
import io.activej.http.AsyncHttpServer;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.service.ServiceGraphModule;

import java.net.InetSocketAddress;

import static io.activej.config.Config.ofClassPathProperties;
import static io.activej.config.Config.ofSystemProperties;
import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;
import static io.activej.di.module.Modules.combine;
import static io.activej.launchers.initializers.Initializers.ofEventloop;
import static io.activej.launchers.initializers.Initializers.ofHttpServer;

/**
 * Preconfigured Http server launcher.
 *
 * @see Launcher
 */
public abstract class HttpServerLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "http-server.properties";

	@Inject
	AsyncHttpServer httpServer;

	@Provides
	Eventloop eventloop(Config config, @Optional ThrottlingController throttlingController) {
		return Eventloop.create()
				.initialize(ofEventloop(config.getChild("eventloop")))
				.initialize(eventloop -> eventloop.withInspector(throttlingController));
	}

	@Provides
	AsyncHttpServer server(Eventloop eventloop, AsyncServlet rootServlet, Config config) {
		return AsyncHttpServer.create(eventloop, rootServlet)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	Config config() {
		return Config.create()
				.with("http.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(8080)))
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
		logger.info("HTTP Server is now available at {}", String.join(", ", httpServer.getHttpAddresses()));
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {

		Launcher launcher = new HttpServerLauncher() {
			@Override
			protected Module getBusinessLogicModule() {
				return new AbstractModule() {
							@Provides
							public AsyncServlet servlet(Config config) {
								String message = config.get("message", "Hello, world!");
								return request -> HttpResponse.ok200().withPlainText(message);
							}
						};
			}
		};

		launcher.launch(args);
	}
}
