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

package io.activej.launchers.remotefs;

import io.activej.async.service.EventloopTaskScheduler;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Optional;
import io.activej.inject.annotation.Provides;
import io.activej.launcher.Launcher;
import io.activej.remotefs.FsClient;
import io.activej.remotefs.RemoteFsServer;
import io.activej.remotefs.cluster.FsPartitions;
import io.activej.remotefs.cluster.RemoteFsRepartitionController;
import io.activej.remotefs.cluster.ServerSelector;

import java.util.HashMap;
import java.util.Map;

import static io.activej.common.Utils.nullToDefault;
import static io.activej.launchers.initializers.Initializers.ofEventloopTaskScheduler;
import static io.activej.launchers.remotefs.Initializers.ofFsPartitions;
import static io.activej.launchers.remotefs.Initializers.ofRepartitionController;
import static io.activej.remotefs.cluster.ServerSelector.RENDEZVOUS_HASH_SHARDER;

public abstract class RepartitionControllerLauncher extends RemoteFsServerLauncher {

	@Inject
    RemoteFsRepartitionController controller;

	@Inject
	@Named("repartition")
	EventloopTaskScheduler repartitionScheduler;

	@Inject
	@Named("clusterDeadCheck")
	EventloopTaskScheduler clusterDeadCheckScheduler;

	@Provides
	@Named("repartition")
	EventloopTaskScheduler eventloopTaskScheduler(Config config, RemoteFsRepartitionController controller) {
		return EventloopTaskScheduler.create(controller.getEventloop(), controller::repartition)
				.withInitializer(ofEventloopTaskScheduler(config.getChild("scheduler.repartition")));
	}

	@Provides
	@Named("clusterDeadCheck")
	EventloopTaskScheduler deadCheckScheduler(Config config, FsPartitions partitions) {
		return EventloopTaskScheduler.create(partitions.getEventloop(), partitions::checkDeadPartitions)
				.withInitializer(ofEventloopTaskScheduler(config.getChild("scheduler.cluster.deadCheck")));
	}

	@Provides
	RemoteFsRepartitionController repartitionController(Config config,
			RemoteFsServer localServer, FsPartitions partitions) {
		String localPartitionId = config.get("remotefs.repartition.localPartitionId");
		return RemoteFsRepartitionController.create(localPartitionId, partitions)
				.withInitializer(ofRepartitionController(config.getChild("remotefs.repartition")));
	}

	@Provides
	FsPartitions fsPartitions(Config config,
			RemoteFsServer localServer, Eventloop eventloop,
			@Optional ServerSelector serverSelector) {
		Map<Object, FsClient> clients = new HashMap<>();
		clients.put(config.get("remotefs.repartition.localPartitionId"), localServer.getClient());
		return FsPartitions.create(eventloop, clients)
				.withServerSelector(nullToDefault(serverSelector, RENDEZVOUS_HASH_SHARDER))
				.withInitializer(ofFsPartitions(config.getChild("remotefs.cluster")));
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new RepartitionControllerLauncher() {};
		launcher.launch(args);
	}
}
