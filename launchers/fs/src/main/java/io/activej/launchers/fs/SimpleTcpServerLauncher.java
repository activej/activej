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
import io.activej.fs.IFileSystem;
import io.activej.fs.FileSystem;
import io.activej.fs.tcp.FileSystemServer;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpServer;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.Module;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.launchers.fs.gui.FileSystemGuiServlet;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;

import static io.activej.config.converter.ConfigConverters.ofPath;
import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.fs.Initializers.ofFileSystemServer;
import static io.activej.launchers.initializers.Initializers.ofEventloop;
import static io.activej.launchers.initializers.Initializers.ofHttpServer;

public class SimpleTcpServerLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "fs-server.properties";
	public static final Path DEFAULT_PATH = Paths.get(System.getProperty("java.io.tmpdir"), "fs-storage");
	public static final String DEFAULT_SERVER_LISTEN_ADDRESS = "*:9000";
	public static final String DEFAULT_GUI_SERVER_LISTEN_ADDRESS = "*:8080";

	@Provides
	public NioReactor reactor(Config config, OptionalDependency<ThrottlingController> throttlingController) {
		return Eventloop.builder()
				.initialize(ofEventloop(config.getChild("eventloop")))
				.withInspector(throttlingController.orElse(null))
				.build();
	}

	@Eager
	@Provides
	FileSystemServer fileSystemServer(NioReactor reactor, IFileSystem fileSystem, Config config) {
		return FileSystemServer.builder(reactor, fileSystem)
				.initialize(ofFileSystemServer(config.getChild("fs")))
				.build();
	}

	@Provides
	@Eager
	HttpServer guiServer(NioReactor reactor, AsyncServlet servlet, Config config) {
		return HttpServer.builder(reactor, servlet)
				.initialize(ofHttpServer(config.getChild("fs.http.gui")))
				.build();
	}

	@Provides
	AsyncServlet guiServlet(Reactor reactor, IFileSystem fileSystem) {
		return FileSystemGuiServlet.create(reactor, fileSystem);
	}

	@Provides
	IFileSystem fileSystem(Reactor reactor, Executor executor, Config config) {
		return FileSystem.create(reactor, executor, config.get(ofPath(), "fs.path", DEFAULT_PATH));
	}

	@Provides
	Executor executor(Config config) {
		return ConfigConverters.getExecutor(config.getChild("fs.executor"));
	}

	@Provides
	Config config() {
		return createConfig()
				.overrideWith(Config.ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(Config.ofSystemProperties("config"));
	}

	protected Config createConfig() {
		return Config.create()
				.with("fs.listenAddresses", DEFAULT_SERVER_LISTEN_ADDRESS)
				.with("fs.http.gui.listenAddresses", DEFAULT_GUI_SERVER_LISTEN_ADDRESS);
	}

	@Override
	protected final Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				JmxModule.create(),
				ConfigModule.builder()
						.withEffectiveConfigLogger()
						.build());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new SimpleTcpServerLauncher().launch(args);
	}
}
