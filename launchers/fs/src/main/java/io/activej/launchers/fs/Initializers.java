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

import io.activej.common.exception.MalformedDataException;
import io.activej.common.initializer.Initializer;
import io.activej.config.Config;
import io.activej.fs.AsyncFs;
import io.activej.fs.LocalFs;
import io.activej.fs.cluster.AsyncDiscoveryService;
import io.activej.fs.cluster.ClusterFs;
import io.activej.fs.cluster.ClusterRepartitionController;
import io.activej.fs.http.HttpFs;
import io.activej.fs.tcp.FsServer;
import io.activej.fs.tcp.RemoteFs;
import io.activej.http.HttpClient;
import io.activej.reactor.nio.NioReactor;
import io.activej.trigger.TriggersModuleSettings;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.activej.common.Checks.checkState;
import static io.activej.common.StringFormatUtils.parseInetSocketAddress;
import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.launchers.initializers.Initializers.ofAbstractServer;
import static io.activej.launchers.initializers.TriggersHelper.ofPromiseStats;
import static io.activej.trigger.Severity.HIGH;
import static io.activej.trigger.Severity.WARNING;

public final class Initializers {

	public static Initializer<FsServer> ofFsServer(Config config) {
		return server -> server
				.withInitializer(ofAbstractServer(config));
	}

	public static Initializer<ClusterRepartitionController> ofClusterRepartitionController(Config config) {
		return controller -> controller
				.withGlob(config.get("glob", "**"))
				.withNegativeGlob(config.get("negativeGlob", ""))
				.withReplicationCount(config.get(ofInteger(), "replicationCount", 1));
	}

	public static AsyncDiscoveryService constantDiscoveryService(NioReactor reactor, Config config) throws MalformedDataException {
		return constantDiscoveryService(reactor, null, config);
	}

	public static AsyncDiscoveryService constantDiscoveryService(NioReactor reactor, @Nullable AsyncFs local, Config config) throws MalformedDataException {
		Map<Object, AsyncFs> partitions = new LinkedHashMap<>();
		partitions.put(config.get("activefs.repartition.localPartitionId"), local);

		List<String> partitionStrings = config.get(ofList(ofString()), "partitions", List.of());
		for (String toAdd : partitionStrings) {
			AsyncFs client;
			if (toAdd.startsWith("http")) {
				client = HttpFs.create(reactor, toAdd, HttpClient.create(reactor));
			} else {
				client = RemoteFs.create(reactor, parseInetSocketAddress(toAdd));
			}
			partitions.put(toAdd, client);
		}

		checkState(!partitions.isEmpty(), "Cluster could not operate without partitions, config had none");
		return AsyncDiscoveryService.constant(partitions);
	}

	public static Initializer<ClusterFs> ofClusterFs(Config config) {
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

	public static Initializer<TriggersModuleSettings> ofLocalFsClient() {
		return triggersModule -> triggersModule
				.with(LocalFs.class, HIGH, "errorUploadBegin", fs -> ofPromiseStats(fs.getUploadBeginPromise()))
				.with(LocalFs.class, HIGH, "errorUploadFinish", fs -> ofPromiseStats(fs.getUploadFinishPromise()))
				.with(LocalFs.class, HIGH, "errorAppendBegin", fs -> ofPromiseStats(fs.getAppendBeginPromise()))
				.with(LocalFs.class, HIGH, "errorAppendFinish", fs -> ofPromiseStats(fs.getAppendFinishPromise()))
				.with(LocalFs.class, HIGH, "errorDownloadBegin", fs -> ofPromiseStats(fs.getDownloadBeginPromise()))
				.with(LocalFs.class, HIGH, "errorDownloadFinish", fs -> ofPromiseStats(fs.getDownloadFinishPromise()))
				.with(LocalFs.class, HIGH, "errorMove", fs -> ofPromiseStats(fs.getMovePromise()))
				.with(LocalFs.class, HIGH, "errorMoveAll", fs -> ofPromiseStats(fs.getMoveAllPromise()))
				.with(LocalFs.class, HIGH, "errorCopy", fs -> ofPromiseStats(fs.getCopyPromise()))
				.with(LocalFs.class, HIGH, "errorCopyAll", fs -> ofPromiseStats(fs.getCopyAllPromise()))
				.with(LocalFs.class, HIGH, "errorList", fs -> ofPromiseStats(fs.getListPromise()))
				.with(LocalFs.class, HIGH, "errorDelete", fs -> ofPromiseStats(fs.getDeletePromise()))
				.with(LocalFs.class, HIGH, "errorDeleteAll", fs -> ofPromiseStats(fs.getDeleteAllPromise()))
				.with(LocalFs.class, HIGH, "errorInfo", fs -> ofPromiseStats(fs.getInfoPromise()))
				.with(LocalFs.class, HIGH, "errorInfoAll", fs -> ofPromiseStats(fs.getInfoAllPromise()));
	}

	public static Initializer<TriggersModuleSettings> ofRemoteFs() {
		return triggersModule -> triggersModule
				.with(RemoteFs.class, WARNING, "errorUploadStart", fs -> ofPromiseStats(fs.getUploadStartPromise()))
				.with(RemoteFs.class, WARNING, "errorUploadFinish", fs -> ofPromiseStats(fs.getUploadFinishPromise()))
				.with(RemoteFs.class, WARNING, "errorDownloadStart", fs -> ofPromiseStats(fs.getDownloadStartPromise()))
				.with(RemoteFs.class, WARNING, "errorDownloadFinish", fs -> ofPromiseStats(fs.getDownloadFinishPromise()))
				.with(RemoteFs.class, WARNING, "errorMove", fs -> ofPromiseStats(fs.getMovePromise()))
				.with(RemoteFs.class, WARNING, "errorMoveAll", fs -> ofPromiseStats(fs.getMoveAllPromise()))
				.with(RemoteFs.class, WARNING, "errorCopy", fs -> ofPromiseStats(fs.getCopyPromise()))
				.with(RemoteFs.class, WARNING, "errorCopyAll", fs -> ofPromiseStats(fs.getCopyAllPromise()))
				.with(RemoteFs.class, WARNING, "errorList", fs -> ofPromiseStats(fs.getListPromise()))
				.with(RemoteFs.class, WARNING, "errorDelete", fs -> ofPromiseStats(fs.getDeletePromise()))
				.with(RemoteFs.class, WARNING, "errorDeleteAll", fs -> ofPromiseStats(fs.getDeleteAllPromise()))
				.with(RemoteFs.class, WARNING, "errorConnect", fs -> ofPromiseStats(fs.getConnectPromise()))
				.with(RemoteFs.class, WARNING, "errorAppendStart", fs -> ofPromiseStats(fs.getAppendStartPromise()))
				.with(RemoteFs.class, WARNING, "errorAppendFinish", fs -> ofPromiseStats(fs.getAppendFinishPromise()))
				.with(RemoteFs.class, WARNING, "errorInfo", fs -> ofPromiseStats(fs.getInfoPromise()))
				.with(RemoteFs.class, WARNING, "errorInfoAll", fs -> ofPromiseStats(fs.getInfoAllPromise()))
				.with(RemoteFs.class, WARNING, "errorPing", fs -> ofPromiseStats(fs.getPingPromise()))
				.with(RemoteFs.class, WARNING, "errorHandshake", fs -> ofPromiseStats(fs.getHandshakePromise()));
	}
}
