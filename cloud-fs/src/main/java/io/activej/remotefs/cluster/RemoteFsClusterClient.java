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
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.ChannelConsumerTransformer;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static io.activej.common.collection.CollectionUtils.transformIterator;
import static io.activej.csp.dsl.ChannelConsumerTransformer.identity;
import static io.activej.remotefs.util.RemoteFsUtils.ofFixedSize;
import static java.util.Collections.emptyMap;
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
	private int uploadTargets = 1;

	// region JMX
	private final PromiseStats uploadStartPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats appendStartPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats appendFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadStartPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats listPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats infoPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats infoAllPromise = PromiseStats.create(Duration.ofMinutes(5));
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
		checkArgument(1 <= replicationCount && replicationCount <= partitions.getClients().size(),
				"Replication count cannot be less than one or more than number of clients");
		this.replicationCount = replicationCount;
		this.uploadTargets = replicationCount;
		return this;
	}

	/**
	 * Sets the replication count as well as number of upload targets that determines the number of server where file will be uploaded.
	 */
	public RemoteFsClusterClient withReplicationCount(int replicationCount, int uploadTargets) {
		checkArgument(1 <= replicationCount && replicationCount <= partitions.getClients().size(),
				"Replication count cannot be less than one or more than number of clients");
		checkArgument(replicationCount <= uploadTargets,
				"Number of upload targets cannot be less than one or more than number of clients");
		this.replicationCount = uploadTargets;
		this.uploadTargets = uploadTargets;
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
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name) {
		return doUpload(name, client -> client.upload(name), identity(), uploadStartPromise, uploadFinishPromise);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name, long size) {
		return doUpload(name, client -> client.upload(name, size), ofFixedSize(size), uploadStartPromise, uploadFinishPromise);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> append(@NotNull String name, long offset) {
		return doUpload(name, client -> client.append(name, offset), identity(), appendStartPromise, appendFinishPromise);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long limit) {
		return broadcast(
				(id, client) -> {
					logger.trace("downloading file {} from {}", name, id);
					return client.download(name, offset, limit)
							.whenException(e -> logger.warn("Failed to connect to a server with key " + id + " to download file " + name, e))
							.map(supplier -> supplier
									.withEndOfStream(eos -> eos
											.thenEx(partitions.wrapDeath(id))));
				},
				AsyncCloseable::close)
				.then(suppliers -> {
					if (suppliers.isEmpty()) {
						return ofFailure("Could not download file '" + name + "' from any server");
					}
					ChannelByteCombiner combiner = ChannelByteCombiner.create();
					for (ChannelSupplier<ByteBuf> supplier : suppliers) {
						combiner.addInput().set(supplier);
					}
					return Promise.of(combiner.getOutput().getSupplier());
				})
				.whenComplete(downloadStartPromise.recordStats());
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target) {
		return FsClient.super.copy(name, target)
				.whenComplete(copyPromise.recordStats());
	}

	@Override
	public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
		return FsClient.super.copyAll(sourceToTarget)
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
		return broadcast(client -> client.delete(name))
				.whenComplete(deletePromise.recordStats())
				.toVoid();
	}

	@Override
	public Promise<Void> deleteAll(Set<String> toDelete) {
		return broadcast(client -> client.deleteAll(toDelete))
				.whenComplete(deleteAllPromise.recordStats())
				.toVoid();
	}

	@Override
	public Promise<Map<String, FileMetadata>> list(@NotNull String glob) {
		return broadcast(client -> client.list(glob))
				.map(maps -> FileMetadata.flatten(maps.stream()))
				.whenComplete(listPromise.recordStats());
	}

	@Override
	public Promise<@Nullable FileMetadata> info(@NotNull String name) {
		return broadcast(client -> client.info(name))
				.map(meta -> meta.stream().max(FileMetadata.COMPARATOR).orElse(null))
				.whenComplete(infoPromise.recordStats());
	}

	@Override
	public Promise<Map<String, @NotNull FileMetadata>> infoAll(@NotNull Set<String> names) {
		if (names.isEmpty()) return Promise.of(emptyMap());

		return broadcast(client -> client.infoAll(names))
				.map(maps -> FileMetadata.flatten(maps.stream()))
				.whenComplete(infoAllPromise.recordStats());
	}

	@Override
	public Promise<Void> ping() {
		return checkNotDead();
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		checkState(uploadTargets >= replicationCount, "Number of upload targets is less than replication count");
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

	private static <T> Promise<T> ofFailure(String message) {
		return Promise.ofException(new StacklessException(RemoteFsClusterClient.class, message));
	}

	private <T> Promise<T> checkStillNotDead(T value) {
		Map<Object, FsClient> deadClients = partitions.getDeadClients();
		if (deadClients.size() >= replicationCount) {
			return ofFailure("There are more dead partitions than replication count(" +
					deadClients.size() + " dead, replication count is " + replicationCount + "), aborting");
		}
		return Promise.of(value);
	}

	private Promise<Void> checkNotDead() {
		return checkStillNotDead(null);
	}

	private Promise<ChannelConsumer<ByteBuf>> doUpload(
			String name,
			Function<FsClient, Promise<ChannelConsumer<ByteBuf>>> action,
			ChannelConsumerTransformer<ByteBuf, ChannelConsumer<ByteBuf>> transformer,
			PromiseStats startStats,
			PromiseStats finishStats) {
		return checkNotDead()
				.then(() -> collect(name, action))
				.then(containers -> {
					ChannelByteSplitter splitter = ChannelByteSplitter.create(replicationCount);
					for (Container<ChannelConsumer<ByteBuf>> container : containers) {
						splitter.addOutput().set(container.value);
					}

					if (logger.isTraceEnabled()) {
						logger.trace("uploading file {} to {}, {}", name, containers.stream()
								.map(container -> container.id.toString())
								.collect(joining(", ", "[", "]")), this);
					}

					return Promise.of(splitter.getInput().getConsumer()
							.transformWith(transformer))
							.whenComplete(finishStats.recordStats());
				})
				.whenComplete(startStats.recordStats());
	}

	private Promise<List<Container<ChannelConsumer<ByteBuf>>>> collect(
			String name,
			Function<FsClient, Promise<ChannelConsumer<ByteBuf>>> action
	) {
		Iterator<Object> idIterator = partitions.select(name).iterator();
		return Promises.toList(
				Stream.generate(() ->
						Promises.firstSuccessful(transformIterator(idIterator,
								id -> call(id, action)
										.map(consumer -> new Container<>(id,
												consumer.withAcknowledgement(ack ->
														ack.thenEx(partitions.wrapDeath(id))))))))
						.limit(uploadTargets))
				.thenEx((containers, e) -> {
					if (e != null) {
						return ofFailure("Didn't connect to enough partitions to upload '" + name + '\'');
					}
					return Promise.of(containers);
				});
	}

	private <T> Promise<T> call(Object id, Function<FsClient, Promise<T>> action) {
		return call(id, ($, client) -> action.apply(client));
	}

	private <T> Promise<T> call(Object id, BiFunction<Object, FsClient, Promise<T>> action) {
		FsClient fsClient = partitions.get(id);
		if (fsClient == null) {  // marked as dead already by somebody
			return Promise.ofException(new StacklessException(RemoteFsClusterClient.class, "Client '" + id + "' is not alive"));
		}
		return action.apply(id, fsClient)
				.thenEx(partitions.wrapDeath(id));
	}

	private <T> Promise<List<T>> broadcast(BiFunction<Object, FsClient, Promise<T>> action, Consumer<T> cleanup) {
		return checkNotDead()
				.then(() -> Promise.<List<Try<T>>>ofCallback(cb ->
						Promises.toList(partitions.getAliveClients().entrySet().stream()
								.map(entry -> action.apply(entry.getKey(), entry.getValue())
										.thenEx(partitions.wrapDeath(entry.getKey()))
										.whenResult(result -> {
											if (cb.isComplete()) {
												cleanup.accept(result);
											}
										})
										.toTry()
										.then(aTry -> checkStillNotDead(aTry)
												.whenException(e -> aTry.ifSuccess(cleanup)))))
								.whenComplete(cb)))
				.map(tries -> tries.stream()
						.filter(Try::isSuccess)
						.map(Try::get)
						.collect(toList()));
	}

	private <T> Promise<List<T>> broadcast(Function<FsClient, Promise<T>> action) {
		return broadcast(($, client) -> action.apply(client), $ -> {});
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
	public int getUploadTargets() {
		return uploadTargets;
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
	public PromiseStats getAppendStartPromise() {
		return appendStartPromise;
	}

	@JmxAttribute
	public PromiseStats getAppendFinishPromise() {
		return appendFinishPromise;
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
	public PromiseStats getInfoPromise() {
		return infoPromise;
	}

	@JmxAttribute
	public PromiseStats getInfoAllPromise() {
		return infoAllPromise;
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
