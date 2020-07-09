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
import io.activej.common.CollectorsEx;
import io.activej.common.api.WithInitializer;
import io.activej.common.collection.Try;
import io.activej.common.exception.StacklessException;
import io.activej.common.exception.UncheckedException;
import io.activej.common.ref.Ref;
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
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.async.function.AsyncSuppliers.reuse;
import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Preconditions.checkState;
import static io.activej.csp.ChannelConsumer.getAcknowledgement;
import static io.activej.remotefs.util.RemoteFsUtils.isWildcard;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toMap;

public final class RemoteFsRepartitionController implements WithInitializer<RemoteFsRepartitionController>, EventloopJmxBeanEx, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsRepartitionController.class);
	private static final boolean CHECK = Check.isEnabled(RemoteFsRepartitionController.class);

	private static final Duration DEFAULT_PLAN_RECALCULATION_INTERVAL = Duration.ofMinutes(1);

	private final Object localPartitionId;
	private final FsClient localStorage;
	private final FsPartitions partitions;
	private final AsyncSupplier<Void> repartition = reuse(this::doRepartition);

	private final List<String> processedFiles = new ArrayList<>();

	private String glob = "**";
	private Predicate<String> negativeGlobPredicate = $ -> true;
	private int replicationCount = 1;
	private long planRecalculationInterval = DEFAULT_PLAN_RECALCULATION_INTERVAL.toMillis();
	private Iterator<String> repartitionPlan;

	private int allFiles = 0;
	private int ensuredFiles = 0;
	private int failedFiles = 0;
	private boolean isRepartitioning;

	private Set<Object> lastAliveClientIds = emptySet();
	private long lastPlanRecalculation;

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
		if (negativeGlob.isEmpty()) {
			return this;
		}
		if (!isWildcard(negativeGlob)) {
			this.negativeGlobPredicate = file -> !file.equals(negativeGlob);
		} else {
			PathMatcher negativeMatcher = FileSystems.getDefault().getPathMatcher("glob:" + negativeGlob);
			this.negativeGlobPredicate = name -> !negativeMatcher.matches(Paths.get(name));
		}
		return this;
	}

	public RemoteFsRepartitionController withReplicationCount(int replicationCount) {
		this.replicationCount = replicationCount;
		return this;
	}

	public RemoteFsRepartitionController withPlanRecalculationInterval(Duration planRecalculationInterval) {
		this.planRecalculationInterval = planRecalculationInterval.toMillis();
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
		processedFiles.clear();
		return recalculatePlan()
				.then(() -> Promises.repeat(
						() -> recalculatePlanIfNeeded()
								.then(() -> {
									if (!repartitionPlan.hasNext()) return Promise.of(false);
									String name = repartitionPlan.next();
											return localStorage.info(name)
											.then(meta -> {
												if (meta == null) {
													logger.warn("File '{}' that should be repartitioned has been deleted", name);
													return Promise.of(false);
												}
												return repartitionFile(name, meta);
											})
											.whenComplete(singleFileRepartitionPromiseStats.recordStats())
											.then(b -> {
												processedFiles.add(name);
												if (b) {
													ensuredFiles++;
												} else {
													failedFiles++;
												}
												return Promise.complete();
											})
											.map($ -> true);
								})))
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

	private Promise<Void> recalculatePlanIfNeeded() {
		if (updateLastAliveClientIds()) {
			return recalculatePlan();
		}
		if (getEventloop().currentTimeMillis() - lastPlanRecalculation > planRecalculationInterval) {
			return recalculatePlan();
		}
		return Promise.complete();
	}

	private Promise<Void> recalculatePlan() {
		return localStorage.list(glob)
				.then(map -> {
					checkEnoughAlivePartitions();

					allFiles = map.size();

					Map<String, FileMetadata> filteredMap = map.entrySet().stream()
							.filter(entry -> negativeGlobPredicate.test(entry.getKey()))
							.filter(entr -> !processedFiles.contains(entr.getKey()))
							.collect(CollectorsEx.toMap());

					Map<Object, Set<String>> groupedById = new HashMap<>();
					for (String name : filteredMap.keySet()) {
						List<Object> selected = partitions.select(name).subList(0, replicationCount);
						selected.remove(localPartitionId); // skip local partition if present
						for (Object id : selected) {
							groupedById.computeIfAbsent(id, $ -> new HashSet<>()).add(name);
						}
					}

					return Promises.reduce(
							groupedById.entrySet().stream()
									.map(entry -> partitions.get(entry.getKey()).infoAll(entry.getValue())
											.whenException(e -> partitions.markIfDead(entry.getKey(), e)))
									.iterator(),
							groupedById.size(),
							filteredMap.entrySet().stream()
									.map(e -> new InfoResults(e.getKey(), e.getValue()))
									.collect(toMap(InfoResults::getName, Function.identity())),
							(result, metas) -> filteredMap.keySet().forEach(name -> result.get(name).remoteMetadata.add(metas.get(name))),
							Map::values)
							.whenResult(results -> {
								repartitionPlan = results.stream()
										.sorted()
										.filter(InfoResults::shouldBeProcessed)
										.map(InfoResults::getName)
										.iterator();

								lastPlanRecalculation = getEventloop().currentTimeMillis();
								updateLastAliveClientIds();
							})
							.thenEx((v, e) -> {
								if (e == null) {
									return Promise.complete();
								} else {
									logger.warn("Failed to recalculate repartition plan, retrying in 1 second", e);
									return Promises.delay(Duration.ofSeconds(1))
											.then(this::recalculatePlan);
								}
							});
				});
	}

	private Promise<Boolean> repartitionFile(String name, FileMetadata meta) {
		partitions.markAlive(localPartitionId); // ensure local partition could also be selected
		checkEnoughAlivePartitions();
		List<Object> selected = partitions.select(name).subList(0, replicationCount);
		return getPartitionsThatNeedOurFile(name, meta, selected)
				.then(uploadTargets -> {
					if (uploadTargets == null) { // null return means failure
						return Promise.of(false);
					}
					if (uploadTargets.isEmpty()) { // everybody had the file
						logger.trace("deleting file {} locally", meta);
						return localStorage.delete(name) // so we delete the copy which does not belong to local partition
								.map($ -> {
									logger.info("handled file {} (ensured on {})", meta, selected);
									return true;
								});
					}
					if (uploadTargets.size() == 1 && uploadTargets.get(0).equals(localPartitionId)) { // everybody had the file AND
						logger.info("handled file {} (ensured on {})", meta, selected);          // we don't delete the local copy
						return Promise.of(true);
					}

					// else we need to upload to at least one non-local partition

					logger.trace("uploading file {} to partitions {}...", meta, uploadTargets);

					ChannelSplitter<ByteBuf> splitter = ChannelSplitter.<ByteBuf>create()
							.withInput(ChannelSupplier.ofPromise(localStorage.download(name)));

					return Promises.toList(uploadTargets.stream() // upload file to target partitions
							.filter(partitionId -> !partitionId.equals(localPartitionId)) // just skip it here
							.map(partitionId -> {
								// upload file to this partition
								FsClient client = partitions.get(partitionId);
								if (client == null) {
									return Promise.ofException(new StacklessException(RemoteFsRepartitionController.class, "Client '" + partitionId + "' is not alive"));
								}
								Ref<Throwable> uploadError = new Ref<>();
								return getAcknowledgement(fn ->
										splitter.addOutput()
												.set(ChannelConsumer.ofPromise(client.upload(name, meta.getSize()))
														.withAcknowledgement(ack -> ack
																.thenEx(($, e) -> {
																	if (e != null && uploadError.get() == null) {
																		uploadError.set(e);
																		logger.warn("failed uploading to partition {}", partitionId, e);
																		partitions.markIfDead(partitionId, e);
																	}
																	// returning complete promise so that other uploads would not fail
																	return fn.apply(Promise.complete());
																}))))
										.then(() -> {
											Throwable error = uploadError.get();
											return error == null ? Promise.complete() : Promise.ofException(error);
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

	private Promise<List<Object>> getPartitionsThatNeedOurFile(String name, FileMetadata fileToUpload, List<Object> selected) {
		InfoResults infoResults = new InfoResults(name, fileToUpload);
		List<Object> ids = new ArrayList<>(selected);
		boolean belongsToLocal = ids.remove(localPartitionId);
		return Promises.toList(ids.stream()
				.map(partitionId -> partitions.get(partitionId)
						.info(name) // checking file existence and size on particular partition
						.whenComplete((meta, e) -> {
							if (e != null) {
								logger.warn("failed connecting to partition {}", partitionId, e);
								partitions.markIfDead(partitionId, e);
								return;
							}
							infoResults.remoteMetadata.add(meta);
						})
						.toTry()))
				.map(tries -> {
					if (!tries.stream().allMatch(Try::isSuccess)) { // any of list calls failed
						logger.warn("failed figuring out partitions for file {}, skipping", fileToUpload);
						return null; // using null to mark failure without exceptions
					}

					if (infoResults.shouldBeUploaded()) {
						List<Object> uploadTargets = new ArrayList<>();
						for (int i = 0; i < infoResults.remoteMetadata.size(); i++) {
							if (infoResults.remoteMetadata.get(i) == null) {
								uploadTargets.add(ids.get(i));
							}
						}
						if (belongsToLocal) {
							// file belongs to local partition
							// adding to upload targets so that it is not deleted later
							uploadTargets.add(localPartitionId);
						}
						return uploadTargets;
					}
					if (infoResults.shouldBeDeleted()) {
						return emptyList();
					}
					return singletonList(localPartitionId);
				});
	}

	/**
	 * @return {@code true} if ids were updated, {@code false} otherwise
	 */
	private boolean updateLastAliveClientIds() {
		Set<Object> aliveClientIds = new HashSet<>(partitions.getAliveClients().keySet());
		if (lastAliveClientIds.equals(aliveClientIds)) {
			return false;
		}
		lastAliveClientIds = aliveClientIds;
		return true;
	}

	private void checkEnoughAlivePartitions() {
		if (partitions.getAliveClients().size() < replicationCount) {
			throw new UncheckedException(new StacklessException(RemoteFsRepartitionController.class, "Not enough alive partitions"));
		}
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

	private static final Comparator<InfoResults> INFO_RESULTS_COMPARATOR =
			Comparator.<InfoResults>comparingLong(infoResults -> infoResults.remoteMetadata.stream()
					.filter(Objects::isNull)
					.count() +
					(infoResults.localMetadata == null ? 0 : 1))
					.thenComparingLong(infoResults -> infoResults.remoteMetadata.stream()
							.filter(Objects::nonNull)
							.findAny().orElse(infoResults.localMetadata)
							.getSize());

	private final class InfoResults implements Comparable<InfoResults> {
		String name;
		@Nullable
		FileMetadata localMetadata;
		final List<@Nullable FileMetadata> remoteMetadata = new ArrayList<>();

		private InfoResults(@NotNull String name, @NotNull FileMetadata localMetadata) {
			this.name = name;
			this.localMetadata = localMetadata;
		}

		public String getName() {
			return name;
		}

		boolean shouldBeProcessed() {
			return shouldBeUploaded() || shouldBeDeleted();
		}

		// file should be uploaded if local file is the most complete file
		// and there are remote partitions that do not have this file or have not a full version
		boolean shouldBeUploaded() {
			return localMetadata != null &&
					remoteMetadata.stream().anyMatch(Objects::isNull);
		}

		// (local) file should be deleted in case all of the remote partitions have a better
		// version of a file
		boolean shouldBeDeleted() {
			return remoteMetadata.size() == replicationCount &&
					remoteMetadata.stream().noneMatch(Objects::isNull);
		}

		@Override
		public int compareTo(@NotNull RemoteFsRepartitionController.InfoResults o) {
			return INFO_RESULTS_COMPARATOR.compare(this, o);
		}
	}
}
