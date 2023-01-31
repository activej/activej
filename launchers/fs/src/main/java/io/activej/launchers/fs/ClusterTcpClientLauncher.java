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

import io.activej.async.service.TaskScheduler;
import io.activej.common.exception.MalformedDataException;
import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.eventloop.Eventloop;
import io.activej.fs.IFileSystem;
import io.activej.fs.cluster.ClusterFileSystem;
import io.activej.fs.cluster.FileSystemPartitions;
import io.activej.fs.cluster.IDiscoveryService;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpServer;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.launchers.fs.gui.FileSystemGuiServlet;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;

import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.fs.Initializers.ofClusterFileSystem;
import static io.activej.launchers.initializers.Initializers.ofHttpServer;
import static io.activej.launchers.initializers.Initializers.ofTaskScheduler;

public class ClusterTcpClientLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "fs-client.properties";

	public static final String DEFAULT_DEAD_CHECK_INTERVAL = "1 seconds";
	public static final String DEFAULT_SERVER_LISTEN_ADDRESS = "*:9000";
	public static final String DEFAULT_GUI_SERVER_LISTEN_ADDRESS = "*:8080";

	@Provides
	NioReactor reactor() {
		return Eventloop.create();
	}

	//[START EXAMPLE]
	@Provides
	@Eager
	@Named("clusterDeadCheck")
	TaskScheduler deadCheckScheduler(Config config, FileSystemPartitions partitions) {
		return TaskScheduler.builder(partitions.getReactor(), partitions::checkDeadPartitions)
				.initialize(ofTaskScheduler(config.getChild("fs.repartition.deadCheck")))
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
		return FileSystemGuiServlet.create(reactor, fileSystem, "Cluster FS Client");
	}

	@Provides
	IFileSystem fileSystem(Reactor reactor, FileSystemPartitions partitions, Config config) {
		return ClusterFileSystem.builder(reactor, partitions)
				.initialize(ofClusterFileSystem(config.getChild("fs.cluster")))
				.build();
	}

	@Provides
	IDiscoveryService discoveryService(NioReactor reactor, Config config) throws MalformedDataException {
		return Initializers.constantDiscoveryService(reactor, config.getChild("fs.cluster"));
	}

	@Provides
	FileSystemPartitions fileSystemPartitions(Reactor reactor, IDiscoveryService discoveryService) {
		return FileSystemPartitions.create(reactor, discoveryService);
	}
	//[END EXAMPLE]

	@Provides
	Config config() {
		return createConfig()
				.overrideWith(Config.ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(Config.ofSystemProperties("config"));
	}

	protected Config createConfig() {
		return Config.create()
				.with("fs.listenAddresses", DEFAULT_SERVER_LISTEN_ADDRESS)
				.with("fs.http.gui.listenAddresses", DEFAULT_GUI_SERVER_LISTEN_ADDRESS)
				.with("fs.repartition.deadCheck.schedule.type", "interval")
				.with("fs.repartition.deadCheck.schedule.value", DEFAULT_DEAD_CHECK_INTERVAL);
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
		new ClusterTcpClientLauncher().launch(args);
	}
}
