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
import io.activej.common.api.WithInitializer;
import io.activej.common.collection.Try;
import io.activej.common.exception.StacklessException;
import io.activej.common.exception.UncheckedException;
import io.activej.common.ref.RefInt;
import io.activej.common.ref.RefLong;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.process.ChannelSplitter;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.jmx.PromiseStats;
import io.activej.remotefs.FileMetadata;
import io.activej.remotefs.FsClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.activej.common.Preconditions.checkArgument;
import static io.activej.common.collection.CollectionUtils.keysToMap;
import static io.activej.common.collection.CollectionUtils.toLimitedString;
import static io.activej.csp.ChannelConsumer.getAcknowledgement;
import static io.activej.promise.Promises.asPromises;
import static io.activej.remotefs.FileMetadata.getMoreCompleteFile;
import static java.util.Collections.emptyList;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * An implementation of {@link FsClient} which operates on a map of other clients as a cluster.
 * Contains some redundancy and fail-safety capabilities.
 */
public final class RemoteFsClusterClient implements FsClient, WithInitializer<RemoteFsClusterClient>, EventloopService, EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsClusterClient.class);

	private final FsPartitions partitions;

	private int replicationCount = 1;

	// region JMX
	private final PromiseStats uploadStartPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadStartPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats listPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats inspectPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats inspectAllPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats copyPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats copyAllPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats movePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats moveAllPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deletePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deleteAllPromise = PromiseStats.create(Duration.ofMinutes(5));
	// endregion

	// region creators
	private RemoteFsClusterClient(FsPartitions partitions) {
		this.partitions = partitions;
	}

	public static RemoteFsClusterClient create(FsPartitions partitions) {
		return new RemoteFsClusterClient(partitions);
	}

	/**
	 * Sets the replication count that determines how many copies of the file should persist over the cluster.
	 */
	public RemoteFsClusterClient withReplicationCount(int replicationCount) {
		checkArgument(1 <= replicationCount && replicationCount <= partitions.getClients().size(), "Replication count cannot be less than one or more than number of clients");
		this.replicationCount = replicationCount;
		return this;
	}
	// endregion

	// region getters
	@NotNull
	@Override
	public Eventloop getEventloop() {
		return partitions.getEventloop();
	}
	// endregion

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String filename) {
		return checkNotDead()
				.then(() -> collect(partitions.select(filename),
						(id, fsClient) -> fsClient.upload(filename)
								.map(consumer -> new Container<>(id,
										consumer.withAcknowledgement(ack ->
												ack.whenException(e -> markIfDead(id, e))))),
						Try::of,
						container -> container.value.close()))
				.then(tries -> {
					List<Container<ChannelConsumer<ByteBuf>>> successes = tries.stream()
							.filter(Try::isSuccess)
							.map(Try::get)
							.collect(toList());

					if (successes.isEmpty()) {
						return ofFailure("Couldn't connect to any partition to upload file " + filename, tries);
					}

					ChannelSplitter<ByteBuf> splitter = ChannelSplitter.<ByteBuf>create().lenient();

					Promise<List<Try<Void>>> uploadResults = Promises.toList(successes.stream()
							.map(container -> getAcknowledgement(fn ->
									splitter.addOutput()
											.set(container.value.withAcknowledgement(fn)))
									.toTry()));

					if (logger.isTraceEnabled()) {
						logger.trace("uploading file {} to {}, {}", filename, successes.stream()
								.map(container -> container.id.toString())
								.collect(joining(", ", "[", "]")), this);
					}

					ChannelConsumer<ByteBuf> consumer = splitter.getInput().getConsumer();

					// check number of uploads only here, so even if there were less connections
					// than replicationCount, they would still upload
					return Promise.of(consumer.withAcknowledgement(ack -> ack
							.then(() -> uploadResults)
							.then(ackTries -> {
								long successCount = ackTries.stream().filter(Try::isSuccess).count();
								// check number of uploads only here, so even if there were less connections
								// than replicationCount, they will still upload
								if (ackTries.size() < replicationCount) {
									return ofFailure("Didn't connect to enough partitions uploading " +
											filename + ", only " + successCount + " finished uploads", ackTries);
								}
								if (successCount < replicationCount) {
									return ofFailure("Couldn't finish uploading file " +
											filename + ", only " + successCount + " acknowledgements received", ackTries);
								}
								return Promise.complete();
							})
							.whenComplete(uploadFinishPromise.recordStats())));
				})
				.whenComplete(uploadStartPromise.recordStats());
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long limit) {
		return checkNotDead()
				.then(() -> getBestIds(name))
				.then(ids -> Promises.firstSuccessful(ids.stream()
						.map(id -> AsyncSupplier.cast(() -> {
							FsClient client = partitions.get(id);
							if (client == null) { // marked as dead already by somebody
								return ofDeadClient(id);
							}
							logger.trace("downloading file {} from {}", name, id);
							return client.download(name, offset, limit)
									.whenException(e -> logger.warn("Failed to connect to a server with key " + id + " to download file " + name, e))
									.thenEx(wrapDeath(id))
									.map(supplier -> supplier
											.withEndOfStream(eos -> eos
													.whenException(e -> markIfDead(id, e))
													.whenComplete(downloadFinishPromise.recordStats())));
						})))
						.thenEx((v, e) -> e == null ?
								Promise.of(v) :
								ofFailure("Could not download file '" + name + "' from any server", emptyList())))
				.whenComplete(downloadStartPromise.recordStats());
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target) {
		return checkNotDead()
				.then(() -> getBestIds(name))
				.then(bestIds -> collect(
						bestIds, (id, client) -> client.copy(name, target),
						tries -> Try.ofException(failedTries("Could not copy file '" + name + "' to '" + target + "' on enough partitions", tries)),
						$ -> {}))
				.toVoid()
				.whenComplete(copyPromise.recordStats());
	}

	@Override
	public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
		return checkNotDead()
				.then(() -> getSubMaps(sourceToTarget))
				.then(subMaps -> {
					Map<String, RefInt> successCounters = keysToMap(sourceToTarget.keySet(), $ -> new RefInt(0));

					return checkNotDead()
							.then(() -> Promises.toList(subMaps.entrySet().stream()
									.map(entry -> {
										FsClient client = partitions.get(entry.getKey());
										if (client == null) { // marked as dead already by somebody
											return ofDeadClient(entry.getKey());
										}
										return client.copyAll(entry.getValue())
												.whenResult(() -> entry.getValue().keySet().stream()
														.map(successCounters::get)
														.forEach(RefInt::inc))
												.thenEx(wrapDeath(entry.getKey()));
									})
									.map(Promise::toTry)))
							.then(tries -> {
								if (successCounters.values().stream()
										.map(RefInt::get)
										.anyMatch(successes -> successes < replicationCount)) {
									return ofFailure("Could not copy files " +
											toLimitedString(sourceToTarget, 50) + " on enough partitions", tries);
								}
								return Promise.complete();
							});
				})
				.whenComplete(copyAllPromise.recordStats());
	}

	@Override
	public Promise<Void> move(@NotNull String name, @NotNull String target) {
		return FsClient.super.move(name, target)
				.whenComplete(movePromise.recordStats());
	}

	@Override
	public Promise<Void> moveAll(Map<String, String> sourceToTarget) {
		return FsClient.super.moveAll(sourceToTarget)
				.whenComplete(moveAllPromise.recordStats());
	}

	@Override
	public Promise<Void> delete(@NotNull String name) {
		return forEachAlive(client -> client.delete(name))
				.whenComplete(deletePromise.recordStats());
	}

	@Override
	public Promise<Void> deleteAll(Set<String> toDelete) {
		return forEachAlive(client -> client.deleteAll(toDelete))
				.whenComplete(deleteAllPromise.recordStats());
	}

	@Override
	public Promise<List<FileMetadata>> list(@NotNull String glob) {
		return checkNotDead()
				.then(() -> Promises.toList(
						partitions.getAliveClients().entrySet().stream()
								.map(entry -> entry.getValue().list(glob)
										.thenEx(wrapDeath(entry.getKey()))
										.toTry())))
				.then(this::checkStillNotDead)
				.map(tries -> FileMetadata.flatten(tries.stream().filter(Try::isSuccess).map(Try::get)))
				.whenComplete(listPromise.recordStats());
	}

	@Override
	public Promise<@Nullable FileMetadata> inspect(@NotNull String name) {
		return checkNotDead()
				.then(() -> Promises.toList(partitions.select(name)
						.stream()
						.map(partitions::get)
						.map(client -> client.inspect(name).toTry())))
				.then(this::checkStillNotDead)
				.then(tries -> {
					List<FileMetadata> successes = tries.stream()
							.filter(Try::isSuccess)
							.map(Try::get)
							.collect(toList());

					if (successes.isEmpty()) {
						return ofFailure("Could not retrieve metadata for file '" + name + "' from any server", tries);
					}

					return Promise.of(successes.stream()
							.filter(Objects::nonNull)
							.max(comparing(FileMetadata::getSize))
							.orElse(null));
				})
				.whenComplete(inspectPromise.recordStats());
	}

	@Override
	public Promise<Map<String, @Nullable FileMetadata>> inspectAll(@NotNull List<String> names) {
		return checkNotDead()
				.then(() -> Promises.toList(partitions.getAliveClients().values()
						.stream()
						.map(client -> client.inspectAll(names).toTry())))
				.then(this::checkStillNotDead)
				.then(tries -> {
					List<Map<String, FileMetadata>> successes = tries.stream()
							.filter(Try::isSuccess)
							.map(Try::get)
							.collect(toList());

					if (successes.isEmpty()) {
						return ofFailure("Could not retrieve metadata for files " + toLimitedString(names, 100) + "' from any server", tries);
					}

					return Promise.of(successes.stream()
							.flatMap(map -> map.entrySet().stream())
							.<Map<String, FileMetadata>>collect(HashMap::new,
									(newMap, entry) -> {
										String key = entry.getKey();
										newMap.put(key, getMoreCompleteFile(newMap.get(key), entry.getValue()));
									},
									($1, $2) -> {
										throw new AssertionError();
									}));
				})
				.whenComplete(inspectAllPromise.recordStats());
	}

	@Override
	public Promise<Void> ping() {
		return checkNotDead();
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return ping();
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	@Override
	public String toString() {
		return "RemoteFsClusterClient{partitions=" + partitions + '}';
	}

	private Promise<Void> forEachAlive(Function<FsClient, Promise<Void>> action) {
		return checkNotDead()
				.then(() -> Promises.all(partitions.getAliveClients().entrySet().stream()
						.map(entry -> action.apply(entry.getValue())
								.thenEx(wrapDeath(entry.getKey()))
								.toTry())));
	}

	// shortcut for creating single Exception from list of possibly failed tries
	private static <T> Throwable failedTries(String message, List<Try<T>> tries) {
		StacklessException exception = new StacklessException(RemoteFsClusterClient.class, message);
		tries.stream()
				.map(Try::getExceptionOrNull)
				.filter(Objects::nonNull)
				.forEach(exception::addSuppressed);
		return exception;
	}

	private static <T, U> Promise<T> ofFailure(String message, List<Try<U>> tries) {
		return Promise.ofException(failedTries(message, tries));
	}

	private static <T> Promise<T> ofDeadClient(Object id) {
		return Promise.ofException(new StacklessException(RemoteFsClusterClient.class, "Client '" + id + "' is not alive"));
	}

	private void markIfDead(Object partitionId, Throwable e) {
		// marking as dead only on lower level connection and other I/O exceptions,
		// stackless exceptions are the ones actually received with an ServerError response (so the node is obviously not dead)
		if (e.getClass() != StacklessException.class) {
			partitions.markDead(partitionId, e);
		}
	}

	private <T> BiFunction<T, Throwable, Promise<T>> wrapDeath(Object partitionId) {
		return (res, e) -> {
			if (e == null) {
				return Promise.of(res);
			}
			markIfDead(partitionId, e);
			return Promise.ofException(new StacklessException(RemoteFsClusterClient.class, "Node failed with exception", e));
		};
	}

	private <T> Promise<List<Try<T>>> checkStillNotDead(List<Try<T>> tries) {
		Map<Object, FsClient> deadClients = partitions.getDeadClients();
		if (deadClients.size() >= replicationCount) {
			return ofFailure("There are more dead partitions than replication count(" +
					deadClients.size() + " dead, replication count is " + replicationCount + "), aborting", tries);
		}
		return Promise.of(tries);
	}

	private Promise<Void> checkNotDead() {
		return checkStillNotDead(emptyList()).toVoid();
	}

	private <T> Promise<List<Try<T>>> collect(Collection<Object> ids, BiFunction<Object, FsClient, Promise<T>> action,
			Function<List<Try<T>>, Try<List<Try<T>>>> finisher, Consumer<T> cleanUp) {
		RefInt successCount = new RefInt(0);
		return Promises.reduceEx(asPromises(ids
						.stream()
						.map(id -> AsyncSupplier.cast(
								() -> {
									FsClient fsClient = partitions.get(id);
									if (fsClient == null) {  // marked as dead already by somebody
										return ofDeadClient(id);
									}
									return action.apply(id, fsClient)
											.thenEx(wrapDeath(id));
								})
								.toTry()
						)),
				$ -> Math.max(0, replicationCount - successCount.get()),
				new ArrayList<>(),
				(result, tryOfTry) -> {
					Try<T> aTry = tryOfTry.get();
					result.add(aTry);
					aTry.ifSuccess($ -> successCount.inc());
					return successCount.get() < replicationCount ? null : Try.of(result);
				},
				finisher,
				aTry -> aTry.ifSuccess(cleanUp));
	}

	private Promise<Set<Object>> getBestIds(@NotNull String name) {
		return Promises.toList(partitions.getAliveClients().entrySet().stream()
				.map(entry -> {
					Object partitionId = entry.getKey();
					return entry.getValue().inspect(name)                         //   ↓ use null's as file non-existence indicators
							.map(res -> res != null ? new Container<>(partitionId, res) : null)
							.thenEx(wrapDeath(partitionId))
							.toTry();
				}))
				.then(this::checkStillNotDead)
				.map(tries -> {

					RefLong bestSize = new RefLong(-1);
					Set<Object> bestIds = tries.stream()
							.filter(Try::isSuccess)
							.map(Try::get)
							.filter(Objects::nonNull)
							.collect(HashSet::new,
									(result, container) -> {
										long size = container.value.getSize();

										if (size == bestSize.get()) {
											result.add(container.id);
										} else if (size > bestSize.get()) {
											result.clear();
											result.add(container.id);
											bestSize.set(size);
										}
									},
									($1, $2) -> {
										throw new AssertionError();
									});

					if (bestIds.isEmpty()) {
						throw new UncheckedException(failedTries("File not found: " + name, tries));
					}

					return bestIds;
				});
	}

	private Promise<Map<Object, Map<String, String>>> getSubMaps(@NotNull Map<String, String> names) {
		return Promises.toList(partitions.getAliveClients().entrySet().stream()
				.map(entry -> {
					Object partitionId = entry.getKey();
					return entry.getValue().inspectAll(new ArrayList<>(names.keySet()))
							.map(res -> new Container<>(partitionId, res))
							.thenEx(wrapDeath(partitionId))
							.toTry();
				}))
				.then(this::checkStillNotDead)
				.map(tries -> {

					Map<String, RefLong> bestSizes = keysToMap(names.keySet(), $ -> new RefLong(-1));
					Map<Object, Map<String, String>> subMaps = tries.stream()
							.filter(Try::isSuccess)
							.map(Try::get)
							.collect(HashMap::new,
									(result, container) -> {
										for (Map.Entry<String, FileMetadata> entry : container.value.entrySet()) {
											if (entry.getValue() == null) {
												continue;
											}
											long size = entry.getValue().getSize();

											RefLong bestSize = bestSizes.get(entry.getKey());
											if (size < bestSize.get()) {
												continue;
											} else if (size > bestSize.get()) {
												result.values().forEach(map -> map.remove(entry.getKey()));
												bestSize.set(size);
											}
											result.computeIfAbsent(container.id, $ -> new HashMap<>())
													.put(entry.getKey(), names.get(entry.getKey()));
										}
									},
									($1, $2) -> {
										throw new AssertionError();
									});

					long collectedNames = subMaps.values().stream()
							.flatMap(map -> map.keySet().stream())
							.distinct()
							.count();
					if (collectedNames < names.size()) {
						throw new UncheckedException(failedTries("Could not find all files: " + toLimitedString(names.keySet(), 100), tries));
					}

					return subMaps;
				});
	}


	private static class Container<T> {
		final Object id;
		final T value;

		Container(Object id, T value) {
			this.id = id;
			this.value = value;
		}
	}

	// region JMX
	@JmxAttribute
	public int getReplicationCount() {
		return replicationCount;
	}

	@JmxAttribute
	public void setReplicationCount(int replicationCount) {
		withReplicationCount(replicationCount);
	}

	@JmxAttribute
	public int getAlivePartitionCount() {
		return partitions.getAliveClients().size();
	}

	@JmxAttribute
	public int getDeadPartitionCount() {
		return partitions.getDeadClients().size();
	}

	@JmxAttribute
	public String[] getAlivePartitions() {
		return partitions.getAliveClients().keySet().stream()
				.map(Object::toString)
				.toArray(String[]::new);
	}

	@JmxAttribute
	public String[] getDeadPartitions() {
		return partitions.getDeadClients().keySet().stream()
				.map(Object::toString)
				.toArray(String[]::new);
	}

	@JmxAttribute
	public PromiseStats getUploadStartPromise() {
		return uploadStartPromise;
	}

	@JmxAttribute
	public PromiseStats getUploadFinishPromise() {
		return uploadFinishPromise;
	}

	@JmxAttribute
	public PromiseStats getDownloadStartPromise() {
		return downloadStartPromise;
	}

	@JmxAttribute
	public PromiseStats getDownloadFinishPromise() {
		return downloadFinishPromise;
	}

	@JmxAttribute
	public PromiseStats getListPromise() {
		return listPromise;
	}

	@JmxAttribute
	public PromiseStats getInspectPromise() {
		return inspectPromise;
	}

	@JmxAttribute
	public PromiseStats getInspectAllPromise() {
		return inspectAllPromise;
	}

	@JmxAttribute
	public PromiseStats getDeletePromise() {
		return deletePromise;
	}

	@JmxAttribute
	public PromiseStats getDeleteAllPromise() {
		return deleteAllPromise;
	}

	@JmxAttribute
	public PromiseStats getCopyPromise() {
		return copyPromise;
	}

	@JmxAttribute
	public PromiseStats getCopyAllPromise() {
		return copyAllPromise;
	}

	@JmxAttribute
	public PromiseStats getMovePromise() {
		return movePromise;
	}

	@JmxAttribute
	public PromiseStats getMoveAllPromise() {
		return moveAllPromise;
	}
	// endregion
}
