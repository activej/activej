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

import io.activej.async.process.AsyncCloseable;
import io.activej.async.service.EventloopService;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.api.WithInitializer;
import io.activej.common.collection.Try;
import io.activej.common.exception.StacklessException;
import io.activej.common.tuple.Tuple2;
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

import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Preconditions.checkArgument;
import static io.activej.common.Preconditions.checkState;
import static io.activej.csp.ChannelConsumer.getAcknowledgement;
import static io.activej.remotefs.cluster.ServerSelector.RENDEZVOUS_HASH_SHARDER;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * An implementation of {@link FsClient} which operates on a map of other clients as a cluster.
 * Contains some redundancy and fail-safety capabilities.
 */
public final class RemoteFsClusterClient implements FsClient, WithInitializer<RemoteFsClusterClient>, EventloopService, EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(RemoteFsClusterClient.class);

	private final Eventloop eventloop;
	private final Map<Object, FsClient> clients;
	private final Map<Object, FsClient> aliveClients = new HashMap<>();
	private final Map<Object, FsClient> deadClients = new HashMap<>();

	private int replicationCount = 1;
	private ServerSelector serverSelector = RENDEZVOUS_HASH_SHARDER;

	// region JMX
	private final PromiseStats connectPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadStartPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadStartPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats movePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats copyPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats listPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deletePromise = PromiseStats.create(Duration.ofMinutes(5));
	// endregion

	// region creators
	private RemoteFsClusterClient(Eventloop eventloop, Map<Object, FsClient> clients) {
		this.eventloop = eventloop;
		this.clients = clients;
		aliveClients.putAll(clients);
	}

	public static RemoteFsClusterClient create(Eventloop eventloop) {
		return new RemoteFsClusterClient(eventloop, new HashMap<>());
	}

	public static RemoteFsClusterClient create(Eventloop eventloop, Map<Object, FsClient> clients) {
		return new RemoteFsClusterClient(eventloop, clients);
	}

	/**
	 * Adds given client with given partition id to this cluster
	 */
	public RemoteFsClusterClient withPartition(Object id, FsClient client) {
		clients.put(id, client);
		aliveClients.put(id, client);
		return this;
	}

	/**
	 * Sets the replication count that determines how many copies of the file should persist over the cluster.
	 */
	public RemoteFsClusterClient withReplicationCount(int replicationCount) {
		checkArgument(1 <= replicationCount && replicationCount <= clients.size(), "Replication count cannot be less than one or more than number of clients");
		this.replicationCount = replicationCount;
		return this;
	}

	/**
	 * Sets the server selection strategy based on file name, alive partitions, and replication count.
	 */
	public RemoteFsClusterClient withServerSelector(@NotNull ServerSelector serverSelector) {
		this.serverSelector = serverSelector;
		return this;
	}
	// endregion

	// region getters
	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	public Map<Object, FsClient> getClients() {
		return Collections.unmodifiableMap(clients);
	}

	public Map<Object, FsClient> getAliveClients() {
		return Collections.unmodifiableMap(aliveClients);
	}

	public Map<Object, FsClient> getDeadClients() {
		return Collections.unmodifiableMap(deadClients);
	}

	public ServerSelector getServerSelector() {
		return serverSelector;
	}
	// endregion

	/**
	 * Starts a check process, which pings all partitions and marks them as dead or alive accordingly
	 *
	 * @return promise of the check
	 */
	public Promise<Void> checkAllPartitions() {
		return Promises.all(
				clients.entrySet().stream()
						.map(entry -> {
							Object id = entry.getKey();
							return entry.getValue()
									.ping()
									.mapEx(($, e) -> {
										if (e == null) {
											markAlive(id);
										} else {
											markDead(id, e);
										}
										return null;
									});
						}))
				.whenComplete(toLogger(logger, "checkAllPartitions"));
	}

	/**
	 * Starts a check process, which pings all dead partitions to possibly mark them as alive.
	 * This is the preferred method as it does nothing when no clients are marked as dead,
	 * and RemoteFS operations themselves do mark nodes as dead on connection failures.
	 *
	 * @return promise of the check
	 */
	public Promise<Void> checkDeadPartitions() {
		return Promises.all(
				deadClients.entrySet().stream()
						.map(entry -> entry.getValue()
								.ping()
								.mapEx(($, e) -> {
									if (e == null) {
										markAlive(entry.getKey());
									}
									return null;
								})))
				.whenComplete(toLogger(logger, "checkDeadPartitions"));
	}

	private void markAlive(Object partitionId) {
		FsClient client = deadClients.remove(partitionId);
		if (client != null) {
			logger.info("Partition {} is alive again!", partitionId);
			aliveClients.put(partitionId, client);
		}
	}

	/**
	 * Mark partition as dead. It means that no operations will use it and it would not be given to the server selector.
	 * Next call of {@link #checkDeadPartitions()} or {@link #checkAllPartitions()} will ping this partition and possibly
	 * mark it as alive again.
	 *
	 * @param partitionId id of the partition to be marked
	 * @param e           optional exception for logging
	 * @return <code>true</code> if partition was alive and <code>false</code> otherwise
	 */
	public boolean markDead(Object partitionId, @Nullable Throwable e) {
		FsClient client = aliveClients.remove(partitionId);
		if (client != null) {
			logger.warn("marking {} as dead ", partitionId, e);
			deadClients.put(partitionId, client);
			return true;
		}
		return false;
	}

	private void markIfDead(Object partitionId, Throwable e) {
		// marking as dead only on lower level connection and other I/O exceptions,
		// remote fs exceptions are the ones actually received with an ServerError response (so the node is obviously not dead)
		if (e.getClass() != StacklessException.class) {
			markDead(partitionId, e);
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

	// shortcut for creating single Exception from list of possibly failed tries
	private static <T, U> Promise<T> ofFailure(String message, List<Try<U>> failed) {
		StacklessException exception = new StacklessException(RemoteFsClusterClient.class, message);
		failed.stream()
				.map(Try::getExceptionOrNull)
				.filter(Objects::nonNull)
				.forEach(exception::addSuppressed);
		return Promise.ofException(exception);
	}

	private Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String filename, @Nullable Long revision) {
		List<Object> selected = serverSelector.selectFrom(filename, aliveClients.keySet(), replicationCount);

		checkState(!selected.isEmpty(), "Selected no servers to upload file " + filename);
		checkState(aliveClients.keySet().containsAll(selected), "Selected an id that is not one of client ids");

		class ConsumerWithId {
			final Object id;
			final ChannelConsumer<ByteBuf> consumer;

			ConsumerWithId(Object id, ChannelConsumer<ByteBuf> consumer) {
				this.id = id;
				this.consumer = consumer;
			}
		}

		return Promises.toList(selected.stream()
				.map(id -> {
					FsClient client = aliveClients.get(id);
					return (revision == null ? client.upload(filename) : client.upload(filename, revision))
							.thenEx(wrapDeath(id))
							.map(consumer -> new ConsumerWithId(id,
									consumer.withAcknowledgement(ack ->
											ack.whenException(e -> markIfDead(id, e)))))
							.toTry();
				}))
				.then(tries -> {
					List<ConsumerWithId> successes = tries.stream()
							.filter(Try::isSuccess)
							.map(Try::get)
							.collect(toList());

					if (successes.isEmpty()) {
						return ofFailure("Couldn't connect to any partition to upload file " + filename, tries);
					}

					ChannelSplitter<ByteBuf> splitter = ChannelSplitter.<ByteBuf>create().lenient();

					Promise<List<Try<Void>>> uploadResults = Promises.toList(successes.stream()
							.map(s1 -> getAcknowledgement(fn ->
									splitter.addOutput()
											.set(s1.consumer.withAcknowledgement(fn)))
									.toTry()));

					if (logger.isTraceEnabled()) {
						logger.trace("uploading file {} to {}, {}", filename, successes.stream().map(s -> s.id.toString()).collect(joining(", ", "[", "]")), this);
					}

					ChannelConsumer<ByteBuf> consumer = splitter.getInput().getConsumer();

					// check number of uploads only here, so even if there were less connections
					// than replicationCount, they will still upload
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
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name) {
		return upload(name, null);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name, long revision) {
		return upload(name, (Long) revision);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long length) {
		if (deadClients.size() >= replicationCount) {
			return ofFailure("There are more dead partitions than replication count(" +
					deadClients.size() + " dead, replication count is " + replicationCount + "), aborting", emptyList());
		}

		return Promises.toList(
				aliveClients.entrySet().stream()
						.map(entry -> {
							Object partitionId = entry.getKey();
							return entry.getValue().getMetadata(name) //   ↓ use null's as file non-existence indicators
									.map(res -> res != null ? new Tuple2<>(partitionId, res) : null)
									.thenEx(wrapDeath(partitionId))
									.toTry();
						}))
				.then(tries -> {
					List<Tuple2<Object, FileMetadata>> successes = tries.stream() // filter successful connections
							.filter(Try::isSuccess)
							.map(Try::get)
							.collect(toList());

					// recheck if our download request marked any partitions as dead
					if (deadClients.size() >= replicationCount) {
						return ofFailure("There are more dead partitions than replication count(" +
								deadClients.size() + " dead, replication count is " + replicationCount + "), aborting", tries);
					}

					// filter partitions where file was found
					List<Tuple2<Object, FileMetadata>> found = successes.stream().filter(Objects::nonNull).collect(toList());

					// find any partition with the biggest file size
					Optional<Tuple2<Object, FileMetadata>> maybeBest = found.stream()
							.max(Comparator.comparing(Tuple2::getValue2, FileMetadata.COMPARATOR));

					if (!maybeBest.isPresent()) {
						return ofFailure("File not found: " + name, tries);
					}
					Tuple2<Object, FileMetadata> best = maybeBest.get();

					return Promises.any(found.stream()
							.filter(piwfs -> piwfs.getValue2().getRevision() == best.getValue2().getRevision())
							.map(piwfs -> {
								FsClient client = aliveClients.get(piwfs.getValue1());
								if (client == null) { // marked as dead already by somebody
									return Promise.ofException(new StacklessException(RemoteFsClusterClient.class, "Client " + piwfs.getValue1() + " is not alive"));
								}
								logger.trace("downloading file {} from {}", name, piwfs.getValue1());
								return client.download(name, offset, length)
										.whenException(e -> logger.warn("Failed to connect to server with key " + piwfs.getValue1() + " to download file " + name, e))
										.thenEx(wrapDeath(piwfs.getValue1()))
										.map(supplier -> supplier
												.withEndOfStream(eos -> eos
														.whenException(e -> markIfDead(piwfs.getValue1(), e))
														.whenComplete(downloadFinishPromise.recordStats())));
							}), AsyncCloseable::close);
				})
				.whenComplete(downloadStartPromise.recordStats());
	}

	@Override
	public Promise<Void> move(@NotNull String name, @NotNull String target, long targetRevision, long tombstoneRevision) {
		if (deadClients.size() >= replicationCount) {
			return ofFailure("There are more dead partitions than replication count(" +
					deadClients.size() + " dead, replication count is " + replicationCount + "), aborting", emptyList());
		}

		return Promises.all(aliveClients.entrySet().stream().map(e -> e.getValue().move(name, target, targetRevision, tombstoneRevision).thenEx(wrapDeath(e.getKey()))))
				.whenComplete(movePromise.recordStats());
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target, long targetRevision) {
		if (deadClients.size() >= replicationCount) {
			return ofFailure("There are more dead partitions than replication count(" +
					deadClients.size() + " dead, replication count is " + replicationCount + "), aborting", emptyList());
		}

		return Promises.all(aliveClients.entrySet().stream().map(e -> e.getValue().copy(name, target, targetRevision).thenEx(wrapDeath(e.getKey()))))
				.whenComplete(copyPromise.recordStats());
	}

	@Override
	public Promise<Void> delete(@NotNull String name, long revision) {
		return Promises.toList(
				aliveClients.entrySet().stream()
						.map(entry -> entry.getValue().delete(name)
								.thenEx(wrapDeath(entry.getKey()))
								.toTry()))
				.then(tries -> {
					if (tries.stream().anyMatch(Try::isSuccess)) { // connected at least to somebody
						return Promise.complete();
					}
					return ofFailure("Couldn't delete on any partition", tries);
				})
				.whenComplete(deletePromise.recordStats());
	}

	private Promise<List<FileMetadata>> doList(@NotNull String glob, BiFunction<FsClient, String, Promise<List<FileMetadata>>> list) {
		if (deadClients.size() >= replicationCount) {
			return ofFailure("There are more dead partitions than replication count(" +
					deadClients.size() + " dead, replication count is " + replicationCount + "), aborting", emptyList());
		}

		// this all is the same as delete, but with list of lists of results, flattened and unified
		return Promises.toList(
				aliveClients.entrySet().stream()
						.map(entry -> list.apply(entry.getValue(), glob)
								.thenEx(wrapDeath(entry.getKey()))
								.toTry()))
				.then(tries -> {
					// recheck if our list request marked any partitions as dead
					if (deadClients.size() >= replicationCount) {
						return ofFailure("There are more dead partitions than replication count(" +
								deadClients.size() + " dead, replication count is " + replicationCount + "), aborting", tries);
					}
					return Promise.of(FileMetadata.flatten(tries.stream().filter(Try::isSuccess).map(Try::get)));
				})
				.whenComplete(listPromise.recordStats());
	}

	@Override
	public Promise<List<FileMetadata>> listEntities(@NotNull String glob) {
		return doList(glob, FsClient::listEntities);
	}

	@Override
	public Promise<List<FileMetadata>> list(@NotNull String glob) {
		return doList(glob, FsClient::list);
	}

	@Override
	public Promise<Void> ping() {
		return checkAllPartitions();
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	@Override
	public String toString() {
		return "RemoteFsClusterClient{clients=" + clients + ", dead=" + deadClients.keySet() + '}';
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
		return aliveClients.size();
	}

	@JmxAttribute
	public int getDeadPartitionCount() {
		return deadClients.size();
	}

	@JmxAttribute
	public String[] getAlivePartitions() {
		return aliveClients.keySet().stream()
				.map(Object::toString)
				.toArray(String[]::new);
	}

	@JmxAttribute
	public String[] getDeadPartitions() {
		return deadClients.keySet().stream()
				.map(Object::toString)
				.toArray(String[]::new);
	}

	@JmxAttribute
	public PromiseStats getConnectPromise() {
		return connectPromise;
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
	public PromiseStats getMovePromise() {
		return movePromise;
	}

	@JmxAttribute
	public PromiseStats getCopyPromise() {
		return copyPromise;
	}

	@JmxAttribute
	public PromiseStats getListPromise() {
		return listPromise;
	}

	@JmxAttribute
	public PromiseStats getDeletePromise() {
		return deletePromise;
	}
	// endregion
}
