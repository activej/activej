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
import io.activej.fs.AsyncFs;
import io.activej.fs.cluster.ClusterRepartitionController;
import io.activej.fs.cluster.AsyncDiscoveryService;
import io.activej.fs.cluster.FsPartitions;
import io.activej.fs.cluster.ServerSelector;
import io.activej.fs.tcp.FsServer;
import io.activej.http.AsyncServlet;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.launchers.fs.gui.FsGuiServlet;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;

import static io.activej.common.Utils.first;
import static io.activej.fs.cluster.ServerSelector.RENDEZVOUS_HASH_SHARDER;
import static io.activej.launchers.fs.Initializers.ofClusterRepartitionController;
import static io.activej.launchers.initializers.Initializers.ofReactorTaskScheduler;

public class ClusterTcpServerLauncher extends SimpleTcpServerLauncher {
	public static final String DEFAULT_DEAD_CHECK_INTERVAL = "1 seconds";
	public static final String DEFAULT_REPARTITION_INTERVAL = "1 seconds";

	//[START EXAMPLE]
	@Provides
	@Eager
	@Named("repartition")
	TaskScheduler repartitionScheduler(
			ClusterRepartitionController controller,
			Config config) {
		return TaskScheduler.create(controller.getReactor(), controller::repartition)
				.withInitializer(ofReactorTaskScheduler(config.getChild("activefs.repartition")));
	}

	@Provides
	@Eager
	@Named("clusterDeadCheck")
	TaskScheduler deadCheckScheduler(Config config, FsPartitions partitions) {
		return TaskScheduler.create(partitions.getReactor(), partitions::checkDeadPartitions)
				.withInitializer(ofReactorTaskScheduler(config.getChild("activefs.repartition.deadCheck")));
	}

	@Provides
	ClusterRepartitionController repartitionController(Reactor reactor,
			FsServer localServer, FsPartitions partitions,
			Config config) {
		String localPartitionId = first(partitions.getAllPartitions());
		assert localPartitionId != null;

		return ClusterRepartitionController.create(reactor, localPartitionId, partitions)
				.withInitializer(ofClusterRepartitionController(config.getChild("activefs.repartition")));
	}

	@Provides
	AsyncDiscoveryService discoveryService(NioReactor reactor,
			AsyncFs activeFs,
			Config config) throws MalformedDataException {
		return Initializers.constantDiscoveryService(reactor, activeFs, config);
	}

	@Provides
	FsPartitions fsPartitions(Reactor reactor, AsyncDiscoveryService discoveryService, OptionalDependency<ServerSelector> serverSelector) {
		return FsPartitions.create(reactor, discoveryService)
				.withServerSelector(serverSelector.orElse(RENDEZVOUS_HASH_SHARDER));
	}
	//[END EXAMPLE]

	@Override
	protected Module getOverrideModule() {
		return new AbstractModule() {
			@Provides
			AsyncServlet guiServlet(AsyncFs fs, ClusterRepartitionController controller) {
				return FsGuiServlet.create(fs, "Cluster server [" + controller.getLocalPartitionId() + ']');
			}
		};
	}

	@Override
	protected Config createConfig() {
		return super.createConfig()
				.with("activefs.repartition.schedule.type", "interval")
				.with("activefs.repartition.schedule.value", DEFAULT_REPARTITION_INTERVAL)
				.with("activefs.repartition.deadCheck.schedule.type", "interval")
				.with("activefs.repartition.deadCheck.schedule.value", DEFAULT_DEAD_CHECK_INTERVAL);
	}

	public static void main(String[] args) throws Exception {
		new ClusterTcpServerLauncher().launch(args);
	}
}
