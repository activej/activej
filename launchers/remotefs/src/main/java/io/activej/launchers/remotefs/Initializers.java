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

import io.activej.common.Initializer;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.remotefs.RemoteFsClient;
import io.activej.remotefs.RemoteFsClusterClient;
import io.activej.remotefs.RemoteFsRepartitionController;
import io.activej.remotefs.RemoteFsServer;

import java.util.Map;

import static io.activej.common.Preconditions.checkState;
import static io.activej.config.Config.THIS;
import static io.activej.config.ConfigConverters.ofInetSocketAddress;
import static io.activej.config.ConfigConverters.ofInteger;
import static io.activej.launchers.initializers.Initializers.ofAbstractServer;

public final class Initializers {

	public static Initializer<RemoteFsServer> ofRemoteFsServer(Config config) {
		return server -> server
				.initialize(ofAbstractServer(config));
	}

	public static Initializer<RemoteFsRepartitionController> ofRepartitionController(Config config) {
		return controller -> controller
				.withGlob(config.get("glob", "**"))
				.withNegativeGlob(config.get("negativeGlob", ""));
	}

	public static Initializer<RemoteFsClusterClient> ofRemoteFsCluster(Eventloop eventloop, Config config) {
		return cluster -> {
			Map<String, Config> partitions = config.getChild("partitions").getChildren();
			checkState(!partitions.isEmpty(), "Cluster could not operate without partitions, config had none");
			for (Map.Entry<String, Config> connection : partitions.entrySet()) {
				cluster.withPartition(connection.getKey(), RemoteFsClient.create(eventloop, connection.getValue().get(ofInetSocketAddress(), THIS)));
			}
			cluster.withReplicationCount(config.get(ofInteger(), "replicationCount", 1));
		};
	}

}
