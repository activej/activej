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

package io.activej.launchers.fs;

import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.config.converter.ConfigConverters;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.inspector.ThrottlingController;
import io.activej.fs.ActiveFs;
import io.activej.fs.LocalActiveFs;
import io.activej.fs.tcp.ActiveFsServer;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Optional;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.service.ServiceGraphModule;

import java.util.concurrent.Executor;

import static io.activej.config.converter.ConfigConverters.ofPath;
import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.fs.Initializers.ofActiveFsServer;
import static io.activej.launchers.initializers.Initializers.ofEventloop;

public abstract class ActiveFsServerLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "activefs-server.properties";

	@Inject
	ActiveFsServer activeFsServer;

	@Provides
	public Eventloop eventloop(Config config, @Optional ThrottlingController throttlingController) {
		return Eventloop.create()
				.withInitializer(ofEventloop(config.getChild("eventloop")))
				.withInitializer(eventloop -> eventloop.withInspector(throttlingController));
	}

	@Provides
	ActiveFsServer activeFsServer(Eventloop eventloop, ActiveFs activeFs, Config config) {
		return ActiveFsServer.create(eventloop, activeFs)
				.withInitializer(ofActiveFsServer(config.getChild("activefs")));
	}

	@Provides
	ActiveFs localActivefs(Eventloop eventloop, Executor executor, Config config){
		return LocalActiveFs.create(eventloop, executor, config.get(ofPath(), "activefs.path"));
	}

	@Provides
	Executor executor(Config config) {
		return ConfigConverters.getExecutor(config.getChild("activefs.executor"));
	}

	@Provides
	Config config() {
		return Config.create()
				.overrideWith(Config.ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(Config.ofSystemProperties("config"));
	}

	@Override
	protected final Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				JmxModule.create(),
				ConfigModule.create()
						.withEffectiveConfigLogger());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new ActiveFsServerLauncher() {};
		launcher.launch(args);
	}
}
