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
import io.activej.async.function.AsyncSuppliers;
import io.activej.async.service.EventloopService;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.api.WithInitializer;
import io.activej.common.collection.Try;
import io.activej.common.exception.StacklessException;
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
import java.util.function.Function;

import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Preconditions.checkArgument;
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
	private final AsyncSupplier<Void> checkAllPartitions = AsyncSuppliers.reuse(this::doCheckAllPartitions);
	private final AsyncSupplier<Void> checkDeadPartitions = AsyncSuppliers.reuse(this::doCheckDeadPartitions);

	private int replicationCount = 1;
	private ServerSelector serverSelector = RENDEZVOUS_HASH_SHARDER;

	// region JMX
	private final PromiseStats uploadStartPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadStartPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats listPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats getMetadataPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deletePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deleteAllPromise = PromiseStats.create(Duration.ofMinutes(5));
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
		return checkAllPartitions.get()
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
		return checkDeadPartitions.get()
				.whenComplete(toLogger(logger, "checkDeadPartitions"));
	}

	/**
	 * Mark partition as dead. It means that no operations will use it and it would not be given to the server selector.
	 * Next call to {@link #checkDeadPartitions()} or {@link #checkAllPartitions()} will ping this partition and possibly
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

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String filename) {
		class ConsumerWithId {
			final Object id;
			final ChannelConsumer<ByteBuf> consumer;

			ConsumerWithId(Object id, ChannelConsumer<ByteBuf> consumer) {
				this.id = id;
				this.consumer = consumer;
			}
		}
		return checkNotDead()
				.then(() -> Promises.toList(serverSelector.selectFrom(filename, aliveClients.keySet())
						.stream()
						.limit(replicationCount)
						.map(id -> aliveClients.get(id).upload(filename)
								.thenEx(wrapDeath(id))
								.map(consumer -> new ConsumerWithId(id,
										consumer.withAcknowledgement(ack ->
												ack.whenException(e -> markIfDead(id, e)))))
								.toTry())))
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
							.map(consumerWithId -> getAcknowledgement(fn ->
									splitter.addOutput()
											.set(consumerWithId.consumer.withAcknowledgement(fn)))
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
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long limit) {
		return checkNotDead()
				.then(() -> Promises.firstSuccessful(serverSelector.selectFrom(name, aliveClients.keySet())
						.stream()
						.map(id -> AsyncSupplier.cast(() -> {
							FsClient client = aliveClients.get(id);
							if (client == null) { // marked as dead already by somebody
								return Promise.ofException(new StacklessException(RemoteFsClusterClient.class, "Client " + id + " is not alive"));
							}
							logger.trace("downloading file {} from {}", name, id);
							return client.download(name, offset, limit)
									.whenException(e -> logger.warn("Failed to connect to server with key " + id + " to download file " + name, e))
									.thenEx(wrapDeath(id))
									.map(supplier -> supplier
											.withEndOfStream(eos -> eos
													.whenException(e -> markIfDead(id, e))
													.whenComplete(downloadFinishPromise.recordStats())));
						}))))
				.thenEx((v, e) -> e == null ?
						Promise.of(v) :
						ofFailure("Could not download file '" + name + "' from any server", emptyList()))
				.whenComplete(downloadStartPromise.recordStats());
	}

	@Override
	public Promise<Void> delete(@NotNull String name) {
		return atLeastForOne(client -> client.delete(name))
				.whenComplete(deletePromise.recordStats());
	}

	@Override
	public Promise<Void> deleteAll(Set<String> toDelete) {
		return atLeastForOne(client -> client.deleteAll(toDelete))
				.whenComplete(deleteAllPromise.recordStats());
	}

	@Override
	public Promise<List<FileMetadata>> list(@NotNull String glob) {
		return checkNotDead()
				.then(() -> Promises.toList(
						aliveClients.entrySet().stream()
								.map(entry -> entry.getValue().list(glob)
										.thenEx(wrapDeath(entry.getKey()))
										.toTry())))
				.then(tries -> checkNotDead()
						.map($ -> FileMetadata.flatten(tries.stream().filter(Try::isSuccess).map(Try::get))))
				.whenComplete(listPromise.recordStats());
	}

	@Override
	public Promise<@Nullable FileMetadata> getMetadata(@NotNull String name) {
		return checkNotDead()
				.then(() -> Promises.firstSuccessful(serverSelector.selectFrom(name, aliveClients.keySet())
						.stream()
						.map(id -> AsyncSupplier.cast(() -> {
							FsClient client = aliveClients.get(id);
							if (client == null) { // marked as dead already by somebody
								return Promise.ofException(new StacklessException(RemoteFsClusterClient.class, "Client " + id + " is not alive"));
							}
							return client.getMetadata(name);
						}))))
				.thenEx((v, e) -> e == null ?
						Promise.of(v) :
						ofFailure("Could not retrieve metadata for file '" + name + "' from any server", emptyList()))
				.whenComplete(getMetadataPromise.recordStats());
	}

	@Override
	public Promise<Void> ping() {
		return checkAllPartitions()
				.then(this::checkNotDead);
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
		return "RemoteFsClusterClient{clients=" + clients + ", dead=" + deadClients.keySet() + '}';
	}

	private Promise<Void> doCheckAllPartitions() {
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
						}));
	}

	private Promise<Void> doCheckDeadPartitions() {
		return Promises.all(
				deadClients.entrySet().stream()
						.map(entry -> entry.getValue()
								.ping()
								.mapEx(($, e) -> {
									if (e == null) {
										markAlive(entry.getKey());
									}
									return null;
								})));
	}

	private Promise<Void> atLeastForOne(Function<FsClient, Promise<Void>> action) {
		return Promises.toList(
				aliveClients.entrySet().stream()
						.map(entry -> action.apply(entry.getValue())
								.thenEx(wrapDeath(entry.getKey()))
								.toTry()))
				.then(tries -> {
					if (tries.stream().anyMatch(Try::isSuccess)) { // connected at least to somebody
						return Promise.complete();
					}
					return ofFailure("Couldn't delete on any partition", tries);
				});
	}

	private void markAlive(Object partitionId) {
		FsClient client = deadClients.remove(partitionId);
		if (client != null) {
			logger.info("Partition {} is alive again!", partitionId);
			aliveClients.put(partitionId, client);
		}
	}

	// shortcut for creating single Exception from list of possibly failed tries
	private static <T, U> Promise<T> ofFailure(String message, List<Try<U>> tries) {
		StacklessException exception = new StacklessException(RemoteFsClusterClient.class, message);
		tries.stream()
				.map(Try::getExceptionOrNull)
				.filter(Objects::nonNull)
				.forEach(exception::addSuppressed);
		return Promise.ofException(exception);
	}

	private void markIfDead(Object partitionId, Throwable e) {
		// marking as dead only on lower level connection and other I/O exceptions,
		// stackless exceptions are the ones actually received with an ServerError response (so the node is obviously not dead)
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

	private Promise<Void> checkNotDead() {
		if (deadClients.size() >= replicationCount) {
			return ofFailure("There are more dead partitions than replication count(" +
					deadClients.size() + " dead, replication count is " + replicationCount + "), aborting", emptyList());
		}
		return Promise.complete();
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
	public PromiseStats getGetMetadataPromise() {
		return getMetadataPromise;
	}

	@JmxAttribute
	public PromiseStats getDeletePromise() {
		return deletePromise;
	}

	@JmxAttribute
	public PromiseStats getDeleteAllPromise() {
		return deleteAllPromise;
	}
	// endregion
}
