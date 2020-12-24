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
import io.activej.common.exception.MalformedDataException;
import io.activej.config.Config;
import io.activej.fs.cluster.ClusterActiveFs;
import io.activej.fs.cluster.ClusterRepartitionController;
import io.activej.fs.cluster.FsPartitions;
import io.activej.fs.tcp.ActiveFsServer;

import java.util.List;

import static io.activej.common.Checks.checkState;
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
			List<String> tcpPartitions = config.get(ofList(ofString()), "partitions", fsPartitions.getAllPartitions());
			try {
				fsPartitions.setPartitions(tcpPartitions);
			} catch (MalformedDataException e) {
				throw new RuntimeException(e);
			}
			checkState(!fsPartitions.getPartitions().isEmpty(),
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
