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

package io.activej.remotefs.cluster;

import io.activej.async.function.AsyncSupplier;
import io.activej.async.service.EventloopService;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.Check;
import io.activej.common.api.WithInitializer;
import io.activej.common.collection.Try;
import io.activej.common.exception.StacklessException;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.process.ChannelSplitter;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.activej.promise.jmx.PromiseStats;
import io.activej.remotefs.FileMetadata;
import io.activej.remotefs.FsClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static io.activej.async.function.AsyncSuppliers.reuse;
import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Preconditions.checkState;
import static io.activej.csp.ChannelConsumer.getAcknowledgement;
import static io.activej.remotefs.RemoteFsUtils.isWildcard;

public final class RemoteFsRepartitionController implements WithInitializer<RemoteFsRepartitionController>, EventloopJmxBeanEx, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsRepartitionController.class);
	private static final boolean CHECK = Check.isEnabled(RemoteFsRepartitionController.class);

	private final Object localPartitionId;
	private final FsClient localStorage;
	private final FsPartitions partitions;
	private final AsyncSupplier<Void> repartition = reuse(this::doRepartition);

	private String glob = "**";
	private String negativeGlob = "";
	private int replicationCount = 1;


	private int allFiles = 0;
	private int ensuredFiles = 0;
	private int failedFiles = 0;
	private boolean isRepartitioning;

	@Nullable
	private SettablePromise<Void> closeCallback;

	private final PromiseStats repartitionPromiseStats = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats singleFileRepartitionPromiseStats = PromiseStats.create(Duration.ofMinutes(5));

	private RemoteFsRepartitionController(Object localPartitionId, FsClient localStorage, FsPartitions partitions) {
		this.localPartitionId = localPartitionId;
		this.localStorage = localStorage;
		this.partitions = partitions;
	}

	public static RemoteFsRepartitionController create(Object localPartitionId, FsPartitions partitions) {
		FsClient local = partitions.getClients().get(localPartitionId);
		return new RemoteFsRepartitionController(localPartitionId, local, partitions);
	}

	public RemoteFsRepartitionController withGlob(@NotNull String glob) {
		this.glob = glob;
		return this;
	}

	public RemoteFsRepartitionController withNegativeGlob(@NotNull String negativeGlob) {
		this.negativeGlob = negativeGlob;
		return this;
	}

	public RemoteFsRepartitionController withReplicationCount(int replicationCount) {
		this.replicationCount = replicationCount;
		return this;
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return partitions.getEventloop();
	}

	public Object getLocalPartitionId() {
		return localPartitionId;
	}

	public FsClient getLocalStorage() {
		return localStorage;
	}

	public FsPartitions getPartitions() {
		return partitions;
	}

	public int getReplicationCount() {
		return replicationCount;
	}

	@NotNull
	public Promise<Void> repartition() {
		return repartition.get();
	}

	@NotNull
	private Promise<Void> doRepartition() {
		if (CHECK)
			checkState(partitions.getEventloop().inEventloopThread(), "Should be called from eventloop thread");

		isRepartitioning = true;
		return localStorage.list(glob)
				.then(list -> {
					allFiles = list.size();
					return Promises.sequence( // just handling all local files sequentially
							filterNot(list.stream(), negativeGlob)
									.map(meta ->
											() -> repartitionFile(meta)
													.whenComplete(singleFileRepartitionPromiseStats.recordStats())
													.then((Boolean success) -> {
														if (success) {
															ensuredFiles++;
														} else {
															failedFiles++;
														}
														return Promise.complete();
													})));
				})
				.whenComplete(() -> isRepartitioning = false)
				.whenComplete(repartitionPromiseStats.recordStats())
				.thenEx(($, e) -> {
					if (e != null) {
						logger.warn("forced repartition finish, {} files ensured, {} errored, {} untouched", ensuredFiles, failedFiles, allFiles - ensuredFiles - failedFiles);
					} else {
						logger.info("repartition finished, {} files ensured, {} errored", ensuredFiles, failedFiles);
					}
					if (closeCallback != null) {
						closeCallback.accept($, e);
					}
					return Promise.complete();
				});
	}

	private Stream<FileMetadata> filterNot(Stream<FileMetadata> stream, String glob) {
		if (glob.isEmpty()) {
			return stream;
		}
		if (!isWildcard(glob)) {
			return stream.filter(file -> !file.getName().equals(negativeGlob));
		}
		PathMatcher negativeMatcher = FileSystems.getDefault().getPathMatcher("glob:" + negativeGlob);
		return stream.filter(file -> !negativeMatcher.matches(Paths.get(file.getName())));
	}

	private Promise<Boolean> repartitionFile(FileMetadata meta) {
		partitions.markAlive(localPartitionId); // ensure local partition could also be selected
		List<Object> selected = partitions.select(meta.getName()).subList(0, replicationCount);
		return getPartitionsThatNeedOurFile(meta, selected)
				.then(uploadTargets -> {
					if (uploadTargets == null) { // null return means failure
						return Promise.of(false);
					}
					String name = meta.getName();
					if (uploadTargets.isEmpty()) { // everybody had the file
						logger.trace("deleting file {} locally", meta);
						return localStorage.delete(name) // so we delete the copy which does not belong to local partition
								.map($ -> {
									logger.info("handled file {} (ensured on {})", meta, selected);
									return true;
								});
					}
					if (uploadTargets.size() == 1 && uploadTargets.get(0) == localPartitionId) { // everybody had the file AND
						logger.info("handled file {} (ensured on {})", meta, selected);          // we don't delete the local copy
						return Promise.of(true);
					}

					// else we need to upload to at least one non-local partition

					logger.trace("uploading file {} to partitions {}...", meta, uploadTargets);

					ChannelSplitter<ByteBuf> splitter = ChannelSplitter.<ByteBuf>create()
							.withInput(ChannelSupplier.ofPromise(localStorage.download(name)));

					return Promises.toList(uploadTargets.stream() // upload file to target partitions
							.filter(partitionId -> partitionId != localPartitionId) // just skip it here
							.map(partitionId -> {
								// upload file to this partition
								FsClient client = partitions.get(partitionId);
								if (client == null) {
									return Promise.ofException(new StacklessException(RemoteFsRepartitionController.class, "Client '" + partitionId + "' is not alive"));
								}
								return getAcknowledgement(fn ->
										splitter.addOutput()
												.set(ChannelConsumer.ofPromise(client.upload(name))
														.withAcknowledgement(fn)))
										.whenException(e -> {
											logger.warn("failed uploading to partition {}", partitionId, e);
											partitions.markDead(partitionId, e);
										})
										.whenResult(() -> logger.trace("file {} uploaded to '{}'", meta, partitionId));
							})
							.map(Promise::toTry))
							.then(tries -> {
								if (!tries.stream().allMatch(Try::isSuccess)) { // if anybody failed uploading then we skip this file
									logger.warn("failed uploading file {}, skipping", meta);
									return Promise.of(false);
								}

								if (uploadTargets.contains(localPartitionId)) { // don't delete local if it was marked
									logger.info("handled file {} (ensured on {}, uploaded to {})", meta, selected, uploadTargets);
									return Promise.of(true);
								}

								logger.trace("deleting file {} on {}", meta, localPartitionId);
								return localStorage.delete(name)
										.map($ -> {
											logger.info("handled file {} (ensured on {}, uploaded to {})", meta, selected, uploadTargets);
											return true;
										});
							});
				})
				.whenComplete(toLogger(logger, TRACE, "repartitionFile", meta));
	}

	private Promise<List<Object>> getPartitionsThatNeedOurFile(FileMetadata fileToUpload, List<Object> selected) {
		List<Object> uploadTargets = new ArrayList<>();
		return Promises.toList(selected.stream()
				.map(partitionId -> {
					if (partitionId == localPartitionId) {
						uploadTargets.add(partitionId); // add it to targets so in repartitionFile we know not to delete local file
						return Promise.of(Try.of(null));  // and skip other logic
					}
					return partitions.get(partitionId)
							.inspect(fileToUpload.getName()) // checking file existence and size on particular partition
							.whenComplete((meta, e) -> {
								if (e != null) {
									logger.warn("failed connecting to partition {}", partitionId, e);
									partitions.markDead(partitionId, e);
									return;
								}
								// ↓ when there is no file or it is worse than ours
								if (meta == null || meta.getSize() < fileToUpload.getSize()) {
									uploadTargets.add(partitionId);
								}
							})
							.toTry();
				}))
				.then(tries -> {
					if (!tries.stream().allMatch(Try::isSuccess)) { // any of list calls failed
						logger.warn("failed figuring out partitions for file {}, skipping", fileToUpload);
						return Promise.of(null); // using null to mark failure without exceptions
					}
					return Promise.of(uploadTargets);
				});
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return isRepartitioning() ?
				Promise.ofCallback(cb -> this.closeCallback = cb) :
				Promise.complete();
	}

	// region JMX
	@JmxOperation(description = "start repartitioning")
	public void startRepartition() {
		repartition();
	}

	@JmxAttribute
	public boolean isRepartitioning() {
		return isRepartitioning;
	}

	@JmxAttribute
	public PromiseStats getRepartitionPromiseStats() {
		return repartitionPromiseStats;
	}

	@JmxAttribute
	public PromiseStats getSingleFileRepartitionPromiseStats() {
		return singleFileRepartitionPromiseStats;
	}

	@JmxAttribute
	public int getLastFilesToRepartition() {
		return allFiles;
	}

	@JmxAttribute
	public int getLastEnsuredFiles() {
		return ensuredFiles;
	}

	@JmxAttribute
	public int getLastFailedFiles() {
		return failedFiles;
	}
	// endregion
}
