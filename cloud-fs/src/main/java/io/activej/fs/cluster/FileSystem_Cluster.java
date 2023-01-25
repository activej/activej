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

import io.activej.async.function.AsyncBiFunction;
import io.activej.async.function.AsyncFunction;
import io.activej.async.process.AsyncCloseable;
import io.activej.async.service.ReactiveService;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.collection.Try;
import io.activej.common.function.FunctionEx;
import io.activej.common.function.SupplierEx;
import io.activej.common.ref.RefBoolean;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.ChannelConsumerTransformer;
import io.activej.fs.IFileSystem;
import io.activej.fs.FileMetadata;
import io.activej.fs.exception.FileSystemIOException;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.jmx.PromiseStats;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.transformIterator;
import static io.activej.csp.dsl.ChannelConsumerTransformer.identity;
import static io.activej.fs.util.RemoteFileSystemUtils.ofFixedSize;
import static io.activej.promise.Promises.first;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * An implementation of {@link IFileSystem} which operates on other partitions as a cluster.
 * Contains some redundancy and fail-safety capabilities.
 * <p>
 * This implementation inherits the most strict limitations of all the file systems in cluster,
 * as well as defines several limitations over those specified in {@link IFileSystem} interface:
 * <ul>
 *     <li>Uploaded files should be immutable</li>
 *     <li>Deletion of files is not guaranteed</li>
 *     <li>Based on previous limitation, moving a file also does not guarantees that source file will be deleted</li>
 *     <li>Paths should not contain existing filenames as part of the path</li>
 * </ul>
 */
public final class FileSystem_Cluster extends AbstractReactive
		implements IFileSystem, ReactiveService, ReactiveJmxBeanWithStats {
	private static final Logger logger = LoggerFactory.getLogger(FileSystem_Cluster.class);

	private final FileSystemPartitions partitions;

	/**
	 * Maximum allowed number of dead partitions, if there are more dead partitions than this number,
	 * the cluster is considered malformed.
	 */
	private int deadPartitionsThreshold = 0;

	/**
	 * Minimum number of uploads that have to succeed in order for upload to be considered successful.
	 */
	private int minUploadTargets = 1;

	/**
	 * Initial number of uploads that are initiated.
	 */
	private int maxUploadTargets = 1;

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

	private FileSystem_Cluster(Reactor reactor, FileSystemPartitions partitions) {
		super(reactor);
		this.partitions = partitions;
	}

	public static FileSystem_Cluster create(Reactor reactor, FileSystemPartitions partitions) {
		return builder(reactor, partitions).build();
	}

	public static Builder builder(Reactor reactor, FileSystemPartitions partitions) {
		return new FileSystem_Cluster(reactor, partitions).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, FileSystem_Cluster> {
		private Builder() {}

		/**
		 * Sets the replication count that determines how many copies of the file should persist over the cluster.
		 */
		public Builder withReplicationCount(int replicationCount) {
			checkNotBuilt(this);
			setReplicationCount(replicationCount);
			return this;
		}

		/**
		 * Sets the number of dead partitions upon reaching which the cluster is considered to be invalid.
		 */
		@SuppressWarnings("UnusedReturnValue")
		public Builder withDeadPartitionsThreshold(int deadPartitionsThreshold) {
			checkNotBuilt(this);
			setDeadPartitionsThreshold(deadPartitionsThreshold);
			return this;
		}

		/**
		 * Sets the minimum number of partitions for files to be uploaded to.
		 */
		@SuppressWarnings("UnusedReturnValue")
		public Builder withMinUploadTargets(int minUploadTargets) {
			checkNotBuilt(this);
			setMinUploadTargets(minUploadTargets);
			return this;
		}

		/**
		 * Sets the maximum number of partitions for files to be uploaded to.
		 */
		@SuppressWarnings("UnusedReturnValue")
		public Builder withMaxUploadTargets(int maxUploadTargets) {
			checkNotBuilt(this);
			setMaxUploadTargets(maxUploadTargets);
			return this;
		}

		@Override
		protected FileSystem_Cluster doBuild() {
			checkArgument(minUploadTargets <= maxUploadTargets,
					"Maximum number of upload targets should be not be less than minimum number of upload targets");
			return FileSystem_Cluster.this;
		}
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name) {
		checkInReactorThread(this);
		return doUpload(name, fs -> fs.upload(name), identity(), uploadStartPromise, uploadFinishPromise);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name, long size) {
		checkInReactorThread(this);
		return doUpload(name, fs -> fs.upload(name, size), ofFixedSize(size), uploadStartPromise, uploadFinishPromise);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> append(String name, long offset) {
		checkInReactorThread(this);
		return doUpload(name, fs -> fs.append(name, offset), identity(), appendStartPromise, appendFinishPromise);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String name, long offset, long limit) {
		checkInReactorThread(this);
		return broadcast(
				(id, fs) -> {
					logger.trace("downloading file {} from {}", name, id);
					return fs.download(name, offset, limit)
							.whenException(e -> logger.warn("Failed to connect to a server with key " + id + " to download file " + name, e))
							.map(supplier -> supplier
									.withEndOfStream(eos -> eos
											.whenException(partitions.wrapDeathFn(id))));
				},
				AsyncCloseable::close)
				.map(filterErrorsFn(() -> {
					throw new FileSystemIOException("Could not download file '" + name + "' from any server");
				}))
				.then(suppliers -> {
					ChannelByteCombiner combiner = ChannelByteCombiner.create();
					for (ChannelSupplier<ByteBuf> supplier : suppliers) {
						combiner.addInput().set(supplier);
					}
					return Promise.of(combiner.getOutput().getSupplier());
				})
				.whenComplete(downloadStartPromise.recordStats());
	}

	@Override
	public Promise<Void> copy(String name, String target) {
		checkInReactorThread(this);
		return IFileSystem.super.copy(name, target)
				.whenComplete(copyPromise.recordStats());
	}

	@Override
	public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
		checkInReactorThread(this);
		return IFileSystem.super.copyAll(sourceToTarget)
				.whenComplete(copyAllPromise.recordStats());
	}

	@Override
	public Promise<Void> move(String name, String target) {
		checkInReactorThread(this);
		return IFileSystem.super.move(name, target)
				.whenComplete(movePromise.recordStats());
	}

	@Override
	public Promise<Void> moveAll(Map<String, String> sourceToTarget) {
		checkInReactorThread(this);
		return IFileSystem.super.moveAll(sourceToTarget)
				.whenComplete(moveAllPromise.recordStats());
	}

	@Override
	public Promise<Void> delete(String name) {
		checkInReactorThread(this);
		return broadcast(fs -> fs.delete(name))
				.whenComplete(deletePromise.recordStats())
				.toVoid();
	}

	@Override
	public Promise<Void> deleteAll(Set<String> toDelete) {
		checkInReactorThread(this);
		return broadcast(fs -> fs.deleteAll(toDelete))
				.whenComplete(deleteAllPromise.recordStats())
				.toVoid();
	}

	@Override
	public Promise<Map<String, FileMetadata>> list(String glob) {
		checkInReactorThread(this);
		return broadcast(fs -> fs.list(glob))
				.map(filterErrorsFn())
				.map(maps -> FileMetadata.flatten(maps.stream()))
				.whenComplete(listPromise.recordStats());
	}

	@Override
	public Promise<@Nullable FileMetadata> info(String name) {
		checkInReactorThread(this);
		return broadcast(fs -> fs.info(name))
				.map(filterErrorsFn())
				.map(meta -> meta.stream().max(FileMetadata.COMPARATOR).orElse(null))
				.whenComplete(infoPromise.recordStats());
	}

	@Override
	public Promise<Map<String, FileMetadata>> infoAll(Set<String> names) {
		checkInReactorThread(this);
		if (names.isEmpty()) return Promise.of(Map.of());

		return broadcast(fs -> fs.infoAll(names))
				.map(filterErrorsFn())
				.map(maps -> FileMetadata.flatten(maps.stream()))
				.whenComplete(infoAllPromise.recordStats());
	}

	@Override
	public Promise<Void> ping() {
		checkInReactorThread(this);
		return partitions.checkAllPartitions()
				.then(this::ensureIsAlive);
	}

	@Override
	public Promise<?> start() {
		checkInReactorThread(this);
		checkArgument(deadPartitionsThreshold < partitions.getPartitions().size(),
				"Dead partitions threshold should be less than number of partitions");
		checkArgument(maxUploadTargets <= partitions.getPartitions().size(),
				"Maximum number of upload targets should not exceed total number of partitions");

		return ping();
	}

	@Override
	public Promise<?> stop() {
		checkInReactorThread(this);
		return Promise.complete();
	}

	@Override
	public String toString() {
		return "FileSystem_Cluster{partitions=" + partitions + '}';
	}

	public boolean isAlive() {
		return partitions.getDeadPartitions().size() <= deadPartitionsThreshold;
	}

	private Promise<Void> ensureIsAlive() {
		return isAlive() ?
				Promise.complete() :
				Promise.ofException(new FileSystemIOException("There are more dead partitions than allowed(" +
						partitions.getDeadPartitions().size() + " dead, threshold is " + deadPartitionsThreshold + "), aborting"));
	}

	private Promise<ChannelConsumer<ByteBuf>> doUpload(
			String name,
			AsyncFunction<IFileSystem, ChannelConsumer<ByteBuf>> action,
			ChannelConsumerTransformer<ByteBuf, ChannelConsumer<ByteBuf>> transformer,
			PromiseStats startStats,
			PromiseStats finishStats) {
		return ensureIsAlive()
				.then(() -> collect(name, action))
				.then(containers -> {
					ChannelByteSplitter splitter = ChannelByteSplitter.create(minUploadTargets);
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
			AsyncFunction<IFileSystem, ChannelConsumer<ByteBuf>> action
	) {
		Iterator<Object> idIterator = partitions.select(name).iterator();
		Set<ChannelConsumer<ByteBuf>> consumers = new HashSet<>();
		RefBoolean failed = new RefBoolean(false);
		return Promises.toList(
						Stream.generate(() -> first(
										transformIterator(idIterator,
												id -> call(id, action)
														.whenResult(consumer -> {
															if (failed.get()) {
																consumer.close();
															} else {
																consumers.add(consumer);
															}
														})
														.map(consumer -> new Container<>(id, consumer.withAcknowledgement(ack -> ack.whenException(partitions.wrapDeathFn(id))))))))
								.limit(maxUploadTargets))
				.whenException(() -> {
					consumers.forEach(AsyncCloseable::close);
					failed.set(true);
					throw new FileSystemIOException("Didn't connect to enough partitions to upload '" + name + '\'');
				});
	}

	private <T> Promise<T> call(Object id, AsyncFunction<IFileSystem, T> action) {
		return call(id, ($, fs) -> action.apply(fs));
	}

	private <T> Promise<T> call(Object id, AsyncBiFunction<Object, IFileSystem, T> action) {
		IFileSystem fs = partitions.get(id);
		if (fs == null) {  // marked as dead already by somebody
			return Promise.ofException(new FileSystemIOException("Partition '" + id + "' is not alive"));
		}
		return action.apply(id, fs)
				.whenException(partitions.wrapDeathFn(id));
	}

	private <T> Promise<List<Try<T>>> broadcast(AsyncBiFunction<Object, IFileSystem, T> action, Consumer<T> cleanup) {
		return ensureIsAlive()
				.then(() -> Promise.ofCallback(cb ->
						Promises.toList(partitions.getAlivePartitions().entrySet().stream()
										.map(entry -> action.apply(entry.getKey(), entry.getValue())
												.whenException(partitions.wrapDeathFn(entry.getKey()))
												.whenResult($ -> cb.isComplete(), cleanup::accept)
												.toTry()
												.then(aTry -> ensureIsAlive()
														.map($ -> aTry)
														.whenException(e -> aTry.ifSuccess(cleanup)))))
								.run(cb)));
	}

	private static <T> FunctionEx<List<Try<T>>, List<T>> filterErrorsFn() {
		return filterErrorsFn(List::of);
	}

	private static <T> FunctionEx<List<Try<T>>, List<T>> filterErrorsFn(SupplierEx<List<T>> fallback) {
		return tries -> {
			List<T> successes = tries.stream().filter(Try::isSuccess).map(Try::get).collect(toList());
			if (!successes.isEmpty()) {
				return successes;
			}

			List<Exception> exceptions = tries.stream().filter(Try::isException).map(Try::getException).toList();
			if (!exceptions.isEmpty()) {
				Exception exception = exceptions.get(0);
				if (exceptions.stream().skip(1).allMatch(e -> e == exception)) {
					throw exception;
				}
			}
			return fallback.get();
		};
	}

	private <T> Promise<List<Try<T>>> broadcast(AsyncFunction<IFileSystem, T> action) {
		return broadcast(($, fs) -> action.apply(fs), $ -> {});
	}

	private record Container<T>(Object id, T value) {}

	// region JMX
	@JmxAttribute
	public int getDeadPartitionsThreshold() {
		return deadPartitionsThreshold;
	}

	@JmxAttribute
	public int getMinUploadTargets() {
		return minUploadTargets;
	}

	@JmxAttribute
	public int getMaxUploadTargets() {
		return maxUploadTargets;
	}

	@JmxOperation
	public void setReplicationCount(int replicationCount) {
		checkArgument(1 <= replicationCount, "Replication count cannot be less than one");
		deadPartitionsThreshold = replicationCount - 1;
		minUploadTargets = replicationCount;
		maxUploadTargets = replicationCount;
	}

	@JmxAttribute
	public void setDeadPartitionsThreshold(int deadPartitionsThreshold) {
		checkArgument(0 <= deadPartitionsThreshold,
				"Dead partitions threshold cannot be less than zero");
		this.deadPartitionsThreshold = deadPartitionsThreshold;
	}

	@JmxAttribute
	public void setMinUploadTargets(int minUploadTargets) {
		checkArgument(0 <= minUploadTargets,
				"Minimum number of upload targets should not be less than zero");
		this.minUploadTargets = minUploadTargets;
	}

	@JmxAttribute
	public void setMaxUploadTargets(int maxUploadTargets) {
		checkArgument(0 < maxUploadTargets,
				"Maximum number of upload targets should be greater than zero");
		this.maxUploadTargets = maxUploadTargets;
	}

	@JmxAttribute
	public int getAlivePartitionCount() {
		return partitions.getAlivePartitions().size();
	}

	@JmxAttribute
	public int getDeadPartitionCount() {
		return partitions.getDeadPartitions().size();
	}

	@JmxAttribute
	public String[] getAlivePartitions() {
		return partitions.getAlivePartitions().keySet().stream()
				.map(Object::toString)
				.toArray(String[]::new);
	}

	@JmxAttribute
	public String[] getDeadPartitions() {
		return partitions.getDeadPartitions().keySet().stream()
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

	@JmxAttribute(name = "")
	public FileSystemPartitions getPartitions() {
		return partitions;
	}
	// endregion
}
