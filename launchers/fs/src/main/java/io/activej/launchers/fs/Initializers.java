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

import io.activej.common.api.Initializer;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.net.SocketSettings;
import io.activej.fs.cluster.ClusterActiveFs;
import io.activej.fs.cluster.ClusterRepartitionController;
import io.activej.fs.cluster.FsPartitions;
import io.activej.fs.http.HttpActiveFs;
import io.activej.fs.tcp.ActiveFsServer;
import io.activej.fs.tcp.RemoteActiveFs;
import io.activej.http.AsyncHttpClient;

import java.time.Duration;
import java.util.Map;

import static io.activej.common.Checks.checkState;
import static io.activej.config.Config.THIS;
import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.launchers.initializers.Initializers.ofAbstractServer;

public final class Initializers {

	public static Initializer<ActiveFsServer> ofActiveFsServer(Config config) {
		return server -> server
				.withInitializer(ofAbstractServer(config));
	}

	public static Initializer<ClusterRepartitionController> ofClusterRepartitionController(Config config) {
		return controller -> controller
				.withGlob(config.get("glob", "**"))
				.withNegativeGlob(config.get("negativeGlob", ""))
				.withReplicationCount(config.get(ofInteger(), "replicationCount", 1));
	}

	public static Initializer<FsPartitions> ofFsPartitions(Config config) {
		return fsPartitions -> {
			Map<String, Config> tcpPartitions = config.getChild("partitions").getChild("tcp").getChildren();
			Eventloop eventloop = fsPartitions.getEventloop();
			for (Map.Entry<String, Config> connection : tcpPartitions.entrySet()) {
				RemoteActiveFs client = RemoteActiveFs.create(eventloop,
						connection.getValue().get(ofInetSocketAddress(), THIS));
				fsPartitions.withPartition(connection.getKey(), client);
			}
			Map<String, Config> httpPartitions = config.getChild("partitions").getChild("http").getChildren();
			for (Map.Entry<String, Config> connection : httpPartitions.entrySet()) {
				AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop)
						.withSocketSettings(SocketSettings.createDefault().withImplIdleTimeout(Duration.ofMinutes(1)));
				HttpActiveFs client = HttpActiveFs.create(connection.getValue().get(ofString(), THIS), httpClient);
				fsPartitions.withPartition(connection.getKey(), client);
			}
			checkState(!tcpPartitions.isEmpty() || !httpPartitions.isEmpty(),
					"Cluster could not operate without partitions, config had none");
		};
	}

	public static Initializer<ClusterActiveFs> ofClusterActiveFs(Config config) {
		return cluster -> {
			Integer replicationCount = config.get(ofInteger(), "replicationCount", null);
			if (replicationCount != null) {
				cluster.withReplicationCount(replicationCount);
			} else {
				cluster.withPersistenceOptions(
						config.get(ofInteger(), "deadPartitionsThreshold", 0),
						config.get(ofInteger(), "uploadTargetsMin", 1),
						config.get(ofInteger(), "uploadTargetsMax", 1)
				);
			}
		};
	}

}
