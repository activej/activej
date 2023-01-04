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

package io.activej.fs.cluster;

import io.activej.async.function.AsyncRunnable;
import io.activej.async.service.ReactiveService;
import io.activej.common.Checks;
import io.activej.common.collection.Try;
import io.activej.common.initializer.WithInitializer;
import io.activej.common.ref.RefInt;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.process.ChannelByteRanger;
import io.activej.fs.ActiveFs;
import io.activej.fs.FileMetadata;
import io.activej.fs.exception.FsIOException;
import io.activej.fs.exception.PathContainsFileException;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.activej.promise.jmx.PromiseStats;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;
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

import static io.activej.async.function.AsyncRunnables.reuse;
import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Checks.*;
import static io.activej.common.Utils.first;
import static io.activej.fs.util.RemoteFsUtils.isWildcard;
import static java.util.stream.Collectors.toMap;

public final class ClusterRepartitionController extends AbstractReactive
		implements WithInitializer<ClusterRepartitionController>, ReactiveJmxBeanWithStats, ReactiveService {
	private static final Logger logger = LoggerFactory.getLogger(ClusterRepartitionController.class);
	private static final boolean CHECK = Checks.isEnabled(ClusterRepartitionController.class);

	private static final Duration DEFAULT_PLAN_RECALCULATION_INTERVAL = Duration.ofMinutes(1);

	private final Object localPartitionId;
	private final FsPartitions partitions;
	private final AsyncRunnable repartition = reuse(this::doRepartition);

	private final List<String> processedFiles = new ArrayList<>();

	private ActiveFs localFs;
	private String glob = "**";
	private Predicate<String> negativeGlobPredicate = $ -> true;
	private int replicationCount = 1;
	private long planRecalculationInterval = DEFAULT_PLAN_RECALCULATION_INTERVAL.toMillis();
	private Iterator<String> repartitionPlan;

	private int allFiles = 0;
	private int ensuredFiles = 0;
	private int failedFiles = 0;
	private boolean isRepartitioning;

	private Set<Object> lastAlivePartitionIds = Set.of();
	private long lastPlanRecalculation;

	private @Nullable SettablePromise<Void> closeCallback;

	private final PromiseStats repartitionPromiseStats = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats singleFileRepartitionPromiseStats = PromiseStats.create(Duration.ofMinutes(5));

	private ClusterRepartitionController(Reactor reactor, Object localPartitionId, FsPartitions partitions) {
		super(reactor);
		this.localPartitionId = localPartitionId;
		this.partitions = partitions;
	}

	public static ClusterRepartitionController create(Reactor reactor, Object localPartitionId, FsPartitions partitions) {
		return new ClusterRepartitionController(reactor, localPartitionId, partitions);
	}

	public ClusterRepartitionController withGlob(String glob) {
		this.glob = glob;
		return this;
	}

	public ClusterRepartitionController withNegativeGlob(String negativeGlob) {
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

	public ClusterRepartitionController withReplicationCount(int replicationCount) {
		this.replicationCount = replicationCount;
		return this;
	}

	public ClusterRepartitionController withPlanRecalculationInterval(Duration planRecalculationInterval) {
		this.planRecalculationInterval = planRecalculationInterval.toMillis();
		return this;
	}

	public Object getLocalPartitionId() {
		return localPartitionId;
	}

	public ActiveFs getLocalFs() {
		return localFs;
	}

	public Promise<Void> repartition() {
		return repartition.run();
	}

	private Promise<Void> doRepartition() {
		if (CHECK)
			checkState(partitions.inReactorThread(), "Should be called from eventloop thread");

		if (replicationCount == 1) {
			Set<Object> partitions = this.partitions.getPartitions().keySet();
			if (partitions.size() == 1 && first(partitions).equals(localPartitionId)) {
				logger.info("Only local partition is known, nowhere to repartition");
				return Promise.complete();
			}
		}

		isRepartitioning = true;
		processedFiles.clear();
		return recalculatePlan()
				.then(() -> Promises.repeat(
						() -> recalculatePlanIfNeeded()
								.then(() -> {
									if (!repartitionPlan.hasNext()) return Promise.of(false);
									String name = repartitionPlan.next();
									return localFs.info(name)
											.thenIfElse(Objects::isNull,
													$ -> {
														logger.warn("File '{}' that should be repartitioned has been deleted", name);
														return Promise.of(false);
													},
													meta -> repartitionFile(name, meta))
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
				.then(($, e) -> {
					if (e != null) {
						logger.warn("forced repartition finish, {} files ensured, {} errored, {} untouched", ensuredFiles, failedFiles, allFiles - ensuredFiles - failedFiles, e);
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
		if (updateLastAlivePartitionIds()) {
			return recalculatePlan();
		}
		if (getReactor().currentTimeMillis() - lastPlanRecalculation > planRecalculationInterval) {
			return recalculatePlan();
		}
		return Promise.complete();
	}

	private Promise<Void> recalculatePlan() {
		return localFs.list(glob)
				.then(map -> {
					checkEnoughAlivePartitions();

					allFiles = map.size();

					Map<String, FileMetadata> filteredMap = map.entrySet().stream()
							.filter(entry -> negativeGlobPredicate.test(entry.getKey()))
							.filter(entry -> !processedFiles.contains(entry.getKey()))
							.collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

					Map<Object, Set<String>> groupedById = new HashMap<>();
					for (String name : filteredMap.keySet()) {
						List<Object> selected = partitions.select(name).subList(0, replicationCount);
						selected.remove(localPartitionId); // skip local partition if present
						for (Object id : selected) {
							groupedById.computeIfAbsent(id, $ -> new HashSet<>()).add(name);
						}
					}

					//noinspection ConstantConditions - get() after select()
					return Promises.reduce(
									filteredMap.entrySet().stream()
											.map(e -> new InfoResults(e.getKey(), e.getValue()))
											.collect(toMap(InfoResults::getName, Function.identity())),
									(result, metas) -> filteredMap.keySet().forEach(name -> result.get(name).remoteMetadata.add(metas.get(name))),
									Map::values,
									groupedById.size(),
									groupedById.entrySet().stream()
											.map(entry -> partitions.get(entry.getKey()).infoAll(entry.getValue())
													.whenException(e -> partitions.markIfDead(entry.getKey(), e)))
											.iterator())
							.whenResult(results -> {
								repartitionPlan = results.stream()
										.sorted()
										.filter(InfoResults::shouldBeProcessed)
										.map(InfoResults::getName)
										.iterator();

								lastPlanRecalculation = getReactor().currentTimeMillis();
								updateLastAlivePartitionIds();
							})
							.toVoid()
							.then(Promise::of,
									e -> {
										logger.warn("Failed to recalculate repartition plan, retrying in 1 second", e);
										return Promises.delay(Duration.ofSeconds(1))
												.then(this::recalculatePlan);
									});
				});
	}

	private Promise<Boolean> repartitionFile(String name, FileMetadata meta) throws FsIOException {
		partitions.markAlive(localPartitionId); // ensure local partition could also be selected
		checkEnoughAlivePartitions();
		List<Object> selected = partitions.select(name).subList(0, replicationCount);
		List<Object> ids = new ArrayList<>(selected);
		boolean belongsToLocal = ids.remove(localPartitionId);
		return getInfoResults(name, meta, ids)
				.thenIfElse(Objects::isNull,
						$ -> Promise.of(false),
						infoResults -> {
							if (infoResults.shouldBeDeleted()) { // everybody had the file
								logger.trace("deleting file {} locally", meta);
								return localFs.delete(name) // so we delete the copy which does not belong to local partition
										.map($ -> {
											logger.info("handled file {} : {} (ensured on {})", name, meta, ids);
											return true;
										});
							}
							if (!infoResults.shouldBeUploaded()) {                             // everybody had the file AND
								logger.trace("handled file {} : {} (ensured on {})", name, meta, ids);     // we don't delete the local copy
								return Promise.of(true);
							}

							// else we need to upload to at least one non-local partition

							logger.trace("uploading file {} to partitions {}...", meta, infoResults);

							//noinspection OptionalGetWithoutIsPresent
							long offset = infoResults.remoteMetadata.stream()
									.mapToLong(metadata -> metadata == null ? 0 : metadata.getSize())
									.min()
									.getAsLong();

							ChannelByteSplitter splitter = ChannelByteSplitter.create(1)
									.withInput(ChannelSupplier.ofPromise(localFs.download(name, offset, meta.getSize())));

							RefInt idx = new RefInt(0);
							return Promises.toList(infoResults.remoteMetadata.stream() // upload file to target partitions
											.map(remoteMeta -> {
												Object partitionId = ids.get(idx.value++);
												if (remoteMeta != null && remoteMeta.getSize() >= meta.getSize()) {
													return Promise.of(Try.of(null));
												}
												// upload file to this partition
												ActiveFs fs = partitions.get(partitionId);
												if (fs == null) {
													return Promise.ofException(new FsIOException("File system '" + partitionId + "' is not alive"));
												}
												return Promise.<Void>ofCallback(cb ->
														splitter.addOutput()
																.set(ChannelConsumer.ofPromise(Promise.complete()
																				.then(() -> remoteMeta == null ?
																						fs.upload(name, meta.getSize()) :
																						fs.append(name, remoteMeta.getSize())
																								.map(consumer -> consumer.transformWith(ChannelByteRanger.drop(remoteMeta.getSize() - offset))))
																				.whenException(PathContainsFileException.class, e -> logger.error("Cluster contains files with clashing paths", e)))
																		.withAcknowledgement(ack -> ack
																				.whenResult(() -> logger.trace("file {} uploaded to '{}'", meta, partitionId))
																				.whenException(e -> {
																					logger.warn("failed uploading to partition {}", partitionId, e);
																					partitions.markIfDead(partitionId, e);
																				})
																				.whenComplete(cb::accept))));
											})
											.map(Promise::toTry))
									.thenIfElse(tries -> !tries.stream().allMatch(Try::isSuccess),
											$ -> {
												logger.warn("failed uploading file {}, skipping", meta);
												return Promise.of(false);
											},
											tries -> {
												if (belongsToLocal) { // don't delete local if it was marked
													logger.info("handled file {} : {} (ensured on {}, uploaded to {})", name, meta, selected, infoResults);
													return Promise.of(true);
												}

												logger.trace("deleting file {} on {}", meta, localPartitionId);
												return localFs.delete(name)
														.map($ -> {
															logger.info("handled file {} : {} (ensured on {}, uploaded to {})", name, meta, selected, infoResults);
															return true;
														});
											});
						})
				.whenComplete(toLogger(logger, TRACE, "repartitionFile", meta));
	}

	private Promise<InfoResults> getInfoResults(String name, FileMetadata fileToUpload, List<Object> selected) {
		InfoResults infoResults = new InfoResults(name, fileToUpload);
		//noinspection ConstantConditions - get() right after select()
		return Promises.toList(selected.stream()
						.map(partitionId -> partitions.get(partitionId)
								.info(name) // checking file existence and size on particular partition
								.whenComplete(
										infoResults.remoteMetadata::add,
										e -> {
											logger.warn("failed connecting to partition {}", partitionId, e);
											partitions.markIfDead(partitionId, e);
										})
								.toTry()))
				.map(tries -> {
					if (!tries.stream().allMatch(Try::isSuccess)) { // any of info calls failed
						logger.warn("failed figuring out partitions for file {}, skipping", fileToUpload);
						return null; // using null to mark failure without exceptions
					}

					return infoResults;
				});
	}

	/**
	 * @return {@code true} if ids were updated, {@code false} otherwise
	 */
	private boolean updateLastAlivePartitionIds() {
		Set<Object> alivePartitionIds = new HashSet<>(partitions.getAlivePartitions().keySet());
		if (lastAlivePartitionIds.equals(alivePartitionIds)) {
			return false;
		}
		lastAlivePartitionIds = alivePartitionIds;
		return true;
	}

	private void checkEnoughAlivePartitions() throws FsIOException {
		if (partitions.getAlivePartitions().size() < replicationCount) {
			throw new FsIOException("Not enough alive partitions");
		}
	}

	@Override
	public Promise<Void> start() {
		this.localFs = checkNotNull(partitions.getPartitions().get(localPartitionId), "Partitions do not contain local partition ID");
		return Promise.complete();
	}

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

	@JmxAttribute(name = "")
	public FsPartitions getPartitions() {
		return partitions;
	}

	@JmxAttribute
	public int getReplicationCount() {
		return replicationCount;
	}

	@JmxAttribute
	public void setReplicationCount(int replicationCount) {
		this.replicationCount = checkArgument(replicationCount, count -> count > 0);
	}
	// endregion

	private static final Comparator<InfoResults> INFO_RESULTS_COMPARATOR =
			Comparator.<InfoResults>comparingLong(infoResults -> infoResults.remoteMetadata.stream()
							.filter(Objects::isNull)
							.count() +
							(infoResults.isLocalMetaTheBest() ? 1 : 0))
					.thenComparingLong(infoResults -> infoResults.remoteMetadata.stream()
							.filter(Objects::nonNull)
							.findAny().orElse(infoResults.localMetadata)
							.getSize());

	private final class InfoResults implements Comparable<InfoResults> {
		final String name;
		final FileMetadata localMetadata;
		final List<@Nullable FileMetadata> remoteMetadata = new ArrayList<>();

		private InfoResults(String name, FileMetadata localMetadata) {
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
			return isLocalMetaTheBest() &&
					remoteMetadata.stream().anyMatch(metadata -> metadata == null || metadata.getSize() < localMetadata.getSize());
		}

		// (local) file should be deleted in case all the remote partitions have a better
		// version of a file
		boolean shouldBeDeleted() {
			return remoteMetadata.size() == replicationCount &&
					remoteMetadata.stream().noneMatch(metadata -> metadata == null || metadata.getSize() < localMetadata.getSize());
		}

		boolean isLocalMetaTheBest() {
			long maxSize = remoteMetadata.stream()
					.filter(Objects::nonNull)
					.mapToLong(FileMetadata::getSize)
					.max().orElse(0);

			return localMetadata.getSize() >= maxSize;
		}

		@SuppressWarnings("NullableProblems")
		@Override
		public int compareTo(ClusterRepartitionController.InfoResults o) {
			return INFO_RESULTS_COMPARATOR.compare(this, o);
		}
	}
}
