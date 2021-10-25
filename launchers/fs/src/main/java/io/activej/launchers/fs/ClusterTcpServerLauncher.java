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

import io.activej.async.service.EventloopTaskScheduler;
import io.activej.common.exception.MalformedDataException;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.fs.ActiveFs;
import io.activej.fs.cluster.ClusterRepartitionController;
import io.activej.fs.cluster.DiscoveryService;
import io.activej.fs.cluster.FsPartitions;
import io.activej.fs.cluster.ServerSelector;
import io.activej.fs.tcp.ActiveFsServer;
import io.activej.http.AsyncServlet;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.launchers.fs.gui.ActiveFsGuiServlet;

import static io.activej.common.Utils.first;
import static io.activej.fs.cluster.ServerSelector.RENDEZVOUS_HASH_SHARDER;
import static io.activej.launchers.fs.Initializers.ofClusterRepartitionController;
import static io.activej.launchers.initializers.Initializers.ofEventloopTaskScheduler;

public class ClusterTcpServerLauncher extends SimpleTcpServerLauncher {
	public static final String DEFAULT_DEAD_CHECK_INTERVAL = "1 seconds";
	public static final String DEFAULT_REPARTITION_INTERVAL = "1 seconds";

	//[START EXAMPLE]
	@Provides
	@Eager
	@Named("repartition")
	EventloopTaskScheduler repartitionScheduler(Config config, ClusterRepartitionController controller) {
		return EventloopTaskScheduler.create(controller.getEventloop(), controller::repartition)
				.withInitializer(ofEventloopTaskScheduler(config.getChild("activefs.repartition")));
	}

	@Provides
	@Eager
	@Named("clusterDeadCheck")
	EventloopTaskScheduler deadCheckScheduler(Config config, FsPartitions partitions) {
		return EventloopTaskScheduler.create(partitions.getEventloop(), partitions::checkDeadPartitions)
				.withInitializer(ofEventloopTaskScheduler(config.getChild("activefs.repartition.deadCheck")));
	}

	@Provides
	ClusterRepartitionController repartitionController(Config config, ActiveFsServer localServer, FsPartitions partitions) {
		String localPartitionId = first(partitions.getAllPartitions());
		assert localPartitionId != null;

		return ClusterRepartitionController.create(localPartitionId, partitions)
				.withInitializer(ofClusterRepartitionController(config.getChild("activefs.repartition")));
	}

	@Provides
	DiscoveryService discoveryService(Eventloop eventloop, ActiveFs activeFs, Config config) throws MalformedDataException {
		return Initializers.constantDiscoveryService(eventloop, activeFs, config);
	}

	@Provides
	FsPartitions fsPartitions(Eventloop eventloop, DiscoveryService discoveryService, OptionalDependency<ServerSelector> maybeServerSelector) {

		return FsPartitions.create(eventloop, discoveryService)
				.withServerSelector(maybeServerSelector.orElse(RENDEZVOUS_HASH_SHARDER));
	}
	//[END EXAMPLE]

	@Override
	protected Module getOverrideModule() {
		return new AbstractModule() {
			@Provides
			AsyncServlet guiServlet(ActiveFs fs, ClusterRepartitionController controller) {
				return ActiveFsGuiServlet.create(fs, "Cluster server [" + controller.getLocalPartitionId() + ']');
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
