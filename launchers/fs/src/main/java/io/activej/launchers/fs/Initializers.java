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
import io.activej.fs.Fs_Local;
import io.activej.fs.cluster.AsyncDiscoveryService;
import io.activej.fs.cluster.ClusterRepartitionController;
import io.activej.fs.cluster.Fs_Cluster;
import io.activej.fs.http.Fs_Http;
import io.activej.fs.tcp.FsServer;
import io.activej.fs.tcp.Fs_Remote;
import io.activej.http.HttpClient_Reactive;
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
				client = Fs_Http.create(reactor, toAdd, HttpClient_Reactive.create(reactor));
			} else {
				client = Fs_Remote.create(reactor, parseInetSocketAddress(toAdd));
			}
			partitions.put(toAdd, client);
		}

		checkState(!partitions.isEmpty(), "Cluster could not operate without partitions, config had none");
		return AsyncDiscoveryService.constant(partitions);
	}

	public static Initializer<Fs_Cluster> ofClusterFs(Config config) {
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
				.with(Fs_Local.class, HIGH, "errorUploadBegin", fs -> ofPromiseStats(fs.getUploadBeginPromise()))
				.with(Fs_Local.class, HIGH, "errorUploadFinish", fs -> ofPromiseStats(fs.getUploadFinishPromise()))
				.with(Fs_Local.class, HIGH, "errorAppendBegin", fs -> ofPromiseStats(fs.getAppendBeginPromise()))
				.with(Fs_Local.class, HIGH, "errorAppendFinish", fs -> ofPromiseStats(fs.getAppendFinishPromise()))
				.with(Fs_Local.class, HIGH, "errorDownloadBegin", fs -> ofPromiseStats(fs.getDownloadBeginPromise()))
				.with(Fs_Local.class, HIGH, "errorDownloadFinish", fs -> ofPromiseStats(fs.getDownloadFinishPromise()))
				.with(Fs_Local.class, HIGH, "errorMove", fs -> ofPromiseStats(fs.getMovePromise()))
				.with(Fs_Local.class, HIGH, "errorMoveAll", fs -> ofPromiseStats(fs.getMoveAllPromise()))
				.with(Fs_Local.class, HIGH, "errorCopy", fs -> ofPromiseStats(fs.getCopyPromise()))
				.with(Fs_Local.class, HIGH, "errorCopyAll", fs -> ofPromiseStats(fs.getCopyAllPromise()))
				.with(Fs_Local.class, HIGH, "errorList", fs -> ofPromiseStats(fs.getListPromise()))
				.with(Fs_Local.class, HIGH, "errorDelete", fs -> ofPromiseStats(fs.getDeletePromise()))
				.with(Fs_Local.class, HIGH, "errorDeleteAll", fs -> ofPromiseStats(fs.getDeleteAllPromise()))
				.with(Fs_Local.class, HIGH, "errorInfo", fs -> ofPromiseStats(fs.getInfoPromise()))
				.with(Fs_Local.class, HIGH, "errorInfoAll", fs -> ofPromiseStats(fs.getInfoAllPromise()));
	}

	public static Initializer<TriggersModuleSettings> ofRemoteFs() {
		return triggersModule -> triggersModule
				.with(Fs_Remote.class, WARNING, "errorUploadStart", fs -> ofPromiseStats(fs.getUploadStartPromise()))
				.with(Fs_Remote.class, WARNING, "errorUploadFinish", fs -> ofPromiseStats(fs.getUploadFinishPromise()))
				.with(Fs_Remote.class, WARNING, "errorDownloadStart", fs -> ofPromiseStats(fs.getDownloadStartPromise()))
				.with(Fs_Remote.class, WARNING, "errorDownloadFinish", fs -> ofPromiseStats(fs.getDownloadFinishPromise()))
				.with(Fs_Remote.class, WARNING, "errorMove", fs -> ofPromiseStats(fs.getMovePromise()))
				.with(Fs_Remote.class, WARNING, "errorMoveAll", fs -> ofPromiseStats(fs.getMoveAllPromise()))
				.with(Fs_Remote.class, WARNING, "errorCopy", fs -> ofPromiseStats(fs.getCopyPromise()))
				.with(Fs_Remote.class, WARNING, "errorCopyAll", fs -> ofPromiseStats(fs.getCopyAllPromise()))
				.with(Fs_Remote.class, WARNING, "errorList", fs -> ofPromiseStats(fs.getListPromise()))
				.with(Fs_Remote.class, WARNING, "errorDelete", fs -> ofPromiseStats(fs.getDeletePromise()))
				.with(Fs_Remote.class, WARNING, "errorDeleteAll", fs -> ofPromiseStats(fs.getDeleteAllPromise()))
				.with(Fs_Remote.class, WARNING, "errorConnect", fs -> ofPromiseStats(fs.getConnectPromise()))
				.with(Fs_Remote.class, WARNING, "errorAppendStart", fs -> ofPromiseStats(fs.getAppendStartPromise()))
				.with(Fs_Remote.class, WARNING, "errorAppendFinish", fs -> ofPromiseStats(fs.getAppendFinishPromise()))
				.with(Fs_Remote.class, WARNING, "errorInfo", fs -> ofPromiseStats(fs.getInfoPromise()))
				.with(Fs_Remote.class, WARNING, "errorInfoAll", fs -> ofPromiseStats(fs.getInfoAllPromise()))
				.with(Fs_Remote.class, WARNING, "errorPing", fs -> ofPromiseStats(fs.getPingPromise()))
				.with(Fs_Remote.class, WARNING, "errorHandshake", fs -> ofPromiseStats(fs.getHandshakePromise()));
	}
}
