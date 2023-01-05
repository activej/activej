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
import io.activej.fs.AsyncFs;
import io.activej.fs.cluster.ClusterFs;
import io.activej.fs.cluster.AsyncDiscoveryService;
import io.activej.fs.cluster.FsPartitions;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpServer;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.launchers.fs.gui.FsGuiServlet;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;

import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.fs.Initializers.ofClusterFs;
import static io.activej.launchers.initializers.Initializers.ofHttpServer;
import static io.activej.launchers.initializers.Initializers.ofReactorTaskScheduler;

public class ClusterTcpClientLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "activefs-client.properties";

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
	TaskScheduler deadCheckScheduler(Config config, FsPartitions partitions) {
		return TaskScheduler.create(partitions.getReactor(), partitions::checkDeadPartitions)
				.withInitializer(ofReactorTaskScheduler(config.getChild("activefs.repartition.deadCheck")));
	}

	@Provides
	@Eager
	HttpServer guiServer(NioReactor reactor, AsyncServlet servlet, Config config) {
		return HttpServer.create(reactor, servlet)
				.withInitializer(ofHttpServer(config.getChild("activefs.http.gui")));
	}

	@Provides
	AsyncServlet guiServlet(AsyncFs activeFs) {
		return FsGuiServlet.create(activeFs, "Cluster FS Client");
	}

	@Provides
	AsyncFs asyncFs(Reactor reactor, FsPartitions partitions, Config config) {
		return ClusterFs.create(reactor, partitions)
				.withInitializer(ofClusterFs(config.getChild("activefs.cluster")));
	}

	@Provides
	AsyncDiscoveryService discoveryService(NioReactor reactor, Config config) throws MalformedDataException {
		return Initializers.constantDiscoveryService(reactor, config.getChild("activefs.cluster"));
	}

	@Provides
	FsPartitions fsPartitions(Reactor reactor, AsyncDiscoveryService discoveryService) {
		return FsPartitions.create(reactor, discoveryService);
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
				.with("activefs.listenAddresses", DEFAULT_SERVER_LISTEN_ADDRESS)
				.with("activefs.http.gui.listenAddresses", DEFAULT_GUI_SERVER_LISTEN_ADDRESS)
				.with("activefs.repartition.deadCheck.schedule.type", "interval")
				.with("activefs.repartition.deadCheck.schedule.value", DEFAULT_DEAD_CHECK_INTERVAL);
	}

	@Override
	protected final Module getModule() {
		return combine(
				ModuleBuilder.create()
						.bind(Reactor.class).to(NioReactor.class)
						.build(),
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
		new ClusterTcpClientLauncher().launch(args);
	}
}
