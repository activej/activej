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

import io.activej.common.annotation.StaticFactories;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.initializer.Initializer;
import io.activej.config.Config;
import io.activej.fs.FileSystem;
import io.activej.fs.IFileSystem;
import io.activej.fs.cluster.ClusterFileSystem;
import io.activej.fs.cluster.ClusterRepartitionController;
import io.activej.fs.cluster.IDiscoveryService;
import io.activej.fs.tcp.FileSystemServer;
import io.activej.fs.tcp.RemoteFileSystem;
import io.activej.reactor.nio.NioReactor;
import io.activej.trigger.TriggersModuleSettings;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.activej.common.Checks.checkState;
import static io.activej.common.StringFormatUtils.parseInetSocketAddressResolving;
import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.launchers.initializers.Initializers.ofAbstractServer;
import static io.activej.launchers.initializers.TriggersHelper.ofPromiseStats;
import static io.activej.trigger.Severity.HIGH;
import static io.activej.trigger.Severity.WARNING;

@StaticFactories(Initializer.class)
public class Initializers {

	public static Initializer<FileSystemServer.Builder> ofFileSystemServer(Config config) {
		return builder -> builder
			.initialize(ofAbstractServer(config));
	}

	public static Initializer<ClusterRepartitionController.Builder> ofClusterRepartitionController(Config config) {
		return builder -> builder
			.withGlob(config.get("glob", "**"))
			.withNegativeGlob(config.get("negativeGlob", ""))
			.withReplicationCount(config.get(ofInteger(), "replicationCount", 1));
	}

	public static IDiscoveryService constantDiscoveryService(NioReactor reactor, Config config) throws MalformedDataException {
		return constantDiscoveryService(reactor, null, config);
	}

	public static IDiscoveryService constantDiscoveryService(NioReactor reactor, @Nullable IFileSystem local, Config config) throws MalformedDataException {
		Map<Object, IFileSystem> partitions = new LinkedHashMap<>();
		partitions.put(config.get("fs.repartition.localPartitionId"), local);

		List<String> partitionStrings = config.get(ofList(ofString()), "partitions", List.of());
		for (String toAdd : partitionStrings) {
			IFileSystem client = RemoteFileSystem.create(reactor, parseInetSocketAddressResolving(toAdd));
			partitions.put(toAdd, client);
		}

		checkState(!partitions.isEmpty(), "Cluster could not operate without partitions, config had none");
		return IDiscoveryService.constant(partitions);
	}

	public static Initializer<ClusterFileSystem.Builder> ofClusterFileSystem(Config config) {
		return builder -> {
			Integer replicationCount = config.get(ofInteger(), "replicationCount", null);
			if (replicationCount != null) {
				builder.withReplicationCount(replicationCount);
			} else {
				builder.withDeadPartitionsThreshold(config.get(ofInteger(), "deadPartitionsThreshold", 0))
					.withMinUploadTargets(config.get(ofInteger(), "uploadTargetsMin", 1))
					.withMaxUploadTargets(config.get(ofInteger(), "uploadTargetsMax", 1));
			}
		};
	}

	public static Initializer<TriggersModuleSettings> ofFileSystem() {
		return triggersModule -> triggersModule
			.with(FileSystem.class, HIGH, "errorUploadBegin", fs -> ofPromiseStats(fs.getUploadBeginPromise()))
			.with(FileSystem.class, HIGH, "errorUploadFinish", fs -> ofPromiseStats(fs.getUploadFinishPromise()))
			.with(FileSystem.class, HIGH, "errorAppendBegin", fs -> ofPromiseStats(fs.getAppendBeginPromise()))
			.with(FileSystem.class, HIGH, "errorAppendFinish", fs -> ofPromiseStats(fs.getAppendFinishPromise()))
			.with(FileSystem.class, HIGH, "errorDownloadBegin", fs -> ofPromiseStats(fs.getDownloadBeginPromise()))
			.with(FileSystem.class, HIGH, "errorDownloadFinish", fs -> ofPromiseStats(fs.getDownloadFinishPromise()))
			.with(FileSystem.class, HIGH, "errorMove", fs -> ofPromiseStats(fs.getMovePromise()))
			.with(FileSystem.class, HIGH, "errorMoveAll", fs -> ofPromiseStats(fs.getMoveAllPromise()))
			.with(FileSystem.class, HIGH, "errorCopy", fs -> ofPromiseStats(fs.getCopyPromise()))
			.with(FileSystem.class, HIGH, "errorCopyAll", fs -> ofPromiseStats(fs.getCopyAllPromise()))
			.with(FileSystem.class, HIGH, "errorList", fs -> ofPromiseStats(fs.getListPromise()))
			.with(FileSystem.class, HIGH, "errorDelete", fs -> ofPromiseStats(fs.getDeletePromise()))
			.with(FileSystem.class, HIGH, "errorDeleteAll", fs -> ofPromiseStats(fs.getDeleteAllPromise()))
			.with(FileSystem.class, HIGH, "errorInfo", fs -> ofPromiseStats(fs.getInfoPromise()))
			.with(FileSystem.class, HIGH, "errorInfoAll", fs -> ofPromiseStats(fs.getInfoAllPromise()));
	}

	public static Initializer<TriggersModuleSettings> ofRemoteFileSystem() {
		return triggersModule -> triggersModule
			.with(RemoteFileSystem.class, WARNING, "errorUploadStart", fs -> ofPromiseStats(fs.getUploadStartPromise()))
			.with(RemoteFileSystem.class, WARNING, "errorUploadFinish", fs -> ofPromiseStats(fs.getUploadFinishPromise()))
			.with(RemoteFileSystem.class, WARNING, "errorDownloadStart", fs -> ofPromiseStats(fs.getDownloadStartPromise()))
			.with(RemoteFileSystem.class, WARNING, "errorDownloadFinish", fs -> ofPromiseStats(fs.getDownloadFinishPromise()))
			.with(RemoteFileSystem.class, WARNING, "errorMove", fs -> ofPromiseStats(fs.getMovePromise()))
			.with(RemoteFileSystem.class, WARNING, "errorMoveAll", fs -> ofPromiseStats(fs.getMoveAllPromise()))
			.with(RemoteFileSystem.class, WARNING, "errorCopy", fs -> ofPromiseStats(fs.getCopyPromise()))
			.with(RemoteFileSystem.class, WARNING, "errorCopyAll", fs -> ofPromiseStats(fs.getCopyAllPromise()))
			.with(RemoteFileSystem.class, WARNING, "errorList", fs -> ofPromiseStats(fs.getListPromise()))
			.with(RemoteFileSystem.class, WARNING, "errorDelete", fs -> ofPromiseStats(fs.getDeletePromise()))
			.with(RemoteFileSystem.class, WARNING, "errorDeleteAll", fs -> ofPromiseStats(fs.getDeleteAllPromise()))
			.with(RemoteFileSystem.class, WARNING, "errorConnect", fs -> ofPromiseStats(fs.getConnectPromise()))
			.with(RemoteFileSystem.class, WARNING, "errorAppendStart", fs -> ofPromiseStats(fs.getAppendStartPromise()))
			.with(RemoteFileSystem.class, WARNING, "errorAppendFinish", fs -> ofPromiseStats(fs.getAppendFinishPromise()))
			.with(RemoteFileSystem.class, WARNING, "errorInfo", fs -> ofPromiseStats(fs.getInfoPromise()))
			.with(RemoteFileSystem.class, WARNING, "errorInfoAll", fs -> ofPromiseStats(fs.getInfoAllPromise()))
			.with(RemoteFileSystem.class, WARNING, "errorPing", fs -> ofPromiseStats(fs.getPingPromise()))
			.with(RemoteFileSystem.class, WARNING, "errorHandshake", fs -> ofPromiseStats(fs.getHandshakePromise()));
	}
}
