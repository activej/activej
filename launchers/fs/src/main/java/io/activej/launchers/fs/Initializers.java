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
import io.activej.fs.ActiveFs;
import io.activej.fs.LocalActiveFs;
import io.activej.fs.cluster.ClusterActiveFs;
import io.activej.fs.cluster.ClusterRepartitionController;
import io.activej.fs.cluster.DiscoveryService;
import io.activej.fs.http.HttpActiveFs;
import io.activej.fs.tcp.ActiveFsServer;
import io.activej.fs.tcp.RemoteActiveFs;
import io.activej.http.ReactiveHttpClient;
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

	public static DiscoveryService constantDiscoveryService(NioReactor reactor, Config config) throws MalformedDataException {
		return constantDiscoveryService(reactor, null, config);
	}

	public static DiscoveryService constantDiscoveryService(NioReactor reactor, @Nullable ActiveFs local, Config config) throws MalformedDataException {
		Map<Object, ActiveFs> partitions = new LinkedHashMap<>();
		partitions.put(config.get("activefs.repartition.localPartitionId"), local);

		List<String> partitionStrings = config.get(ofList(ofString()), "partitions", List.of());
		for (String toAdd : partitionStrings) {
			ActiveFs client;
			if (toAdd.startsWith("http")) {
				client = HttpActiveFs.create(reactor, toAdd, ReactiveHttpClient.create(reactor));
			} else {
				client = RemoteActiveFs.create(reactor, parseInetSocketAddress(toAdd));
			}
			partitions.put(toAdd, client);
		}

		checkState(!partitions.isEmpty(), "Cluster could not operate without partitions, config had none");
		return DiscoveryService.constant(partitions);
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

	public static Initializer<TriggersModuleSettings> ofLocalFsClient() {
		return triggersModule -> triggersModule
				.with(LocalActiveFs.class, HIGH, "errorUploadBegin", fs -> ofPromiseStats(fs.getUploadBeginPromise()))
				.with(LocalActiveFs.class, HIGH, "errorUploadFinish", fs -> ofPromiseStats(fs.getUploadFinishPromise()))
				.with(LocalActiveFs.class, HIGH, "errorAppendBegin", fs -> ofPromiseStats(fs.getAppendBeginPromise()))
				.with(LocalActiveFs.class, HIGH, "errorAppendFinish", fs -> ofPromiseStats(fs.getAppendFinishPromise()))
				.with(LocalActiveFs.class, HIGH, "errorDownloadBegin", fs -> ofPromiseStats(fs.getDownloadBeginPromise()))
				.with(LocalActiveFs.class, HIGH, "errorDownloadFinish", fs -> ofPromiseStats(fs.getDownloadFinishPromise()))
				.with(LocalActiveFs.class, HIGH, "errorMove", fs -> ofPromiseStats(fs.getMovePromise()))
				.with(LocalActiveFs.class, HIGH, "errorMoveAll", fs -> ofPromiseStats(fs.getMoveAllPromise()))
				.with(LocalActiveFs.class, HIGH, "errorCopy", fs -> ofPromiseStats(fs.getCopyPromise()))
				.with(LocalActiveFs.class, HIGH, "errorCopyAll", fs -> ofPromiseStats(fs.getCopyAllPromise()))
				.with(LocalActiveFs.class, HIGH, "errorList", fs -> ofPromiseStats(fs.getListPromise()))
				.with(LocalActiveFs.class, HIGH, "errorDelete", fs -> ofPromiseStats(fs.getDeletePromise()))
				.with(LocalActiveFs.class, HIGH, "errorDeleteAll", fs -> ofPromiseStats(fs.getDeleteAllPromise()))
				.with(LocalActiveFs.class, HIGH, "errorInfo", fs -> ofPromiseStats(fs.getInfoPromise()))
				.with(LocalActiveFs.class, HIGH, "errorInfoAll", fs -> ofPromiseStats(fs.getInfoAllPromise()));
	}

	public static Initializer<TriggersModuleSettings> ofRemoteActiveFs() {
		return triggersModule -> triggersModule
				.with(RemoteActiveFs.class, WARNING, "errorUploadStart", fs -> ofPromiseStats(fs.getUploadStartPromise()))
				.with(RemoteActiveFs.class, WARNING, "errorUploadFinish", fs -> ofPromiseStats(fs.getUploadFinishPromise()))
				.with(RemoteActiveFs.class, WARNING, "errorDownloadStart", fs -> ofPromiseStats(fs.getDownloadStartPromise()))
				.with(RemoteActiveFs.class, WARNING, "errorDownloadFinish", fs -> ofPromiseStats(fs.getDownloadFinishPromise()))
				.with(RemoteActiveFs.class, WARNING, "errorMove", fs -> ofPromiseStats(fs.getMovePromise()))
				.with(RemoteActiveFs.class, WARNING, "errorMoveAll", fs -> ofPromiseStats(fs.getMoveAllPromise()))
				.with(RemoteActiveFs.class, WARNING, "errorCopy", fs -> ofPromiseStats(fs.getCopyPromise()))
				.with(RemoteActiveFs.class, WARNING, "errorCopyAll", fs -> ofPromiseStats(fs.getCopyAllPromise()))
				.with(RemoteActiveFs.class, WARNING, "errorList", fs -> ofPromiseStats(fs.getListPromise()))
				.with(RemoteActiveFs.class, WARNING, "errorDelete", fs -> ofPromiseStats(fs.getDeletePromise()))
				.with(RemoteActiveFs.class, WARNING, "errorDeleteAll", fs -> ofPromiseStats(fs.getDeleteAllPromise()))
				.with(RemoteActiveFs.class, WARNING, "errorConnect", fs -> ofPromiseStats(fs.getConnectPromise()))
				.with(RemoteActiveFs.class, WARNING, "errorAppendStart", fs -> ofPromiseStats(fs.getAppendStartPromise()))
				.with(RemoteActiveFs.class, WARNING, "errorAppendFinish", fs -> ofPromiseStats(fs.getAppendFinishPromise()))
				.with(RemoteActiveFs.class, WARNING, "errorInfo", fs -> ofPromiseStats(fs.getInfoPromise()))
				.with(RemoteActiveFs.class, WARNING, "errorInfoAll", fs -> ofPromiseStats(fs.getInfoAllPromise()))
				.with(RemoteActiveFs.class, WARNING, "errorPing", fs -> ofPromiseStats(fs.getPingPromise()))
				.with(RemoteActiveFs.class, WARNING, "errorHandshake", fs -> ofPromiseStats(fs.getHandshakePromise()));
	}
}
