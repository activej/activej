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

package io.activej.crdt.storage.cluster;

import io.activej.async.function.AsyncFunction;
import io.activej.async.function.AsyncSupplier;
import io.activej.async.process.AsyncCloseable;
import io.activej.async.service.EventloopService;
import io.activej.common.collection.Try;
import io.activej.common.initializer.WithInitializer;
import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtException;
import io.activej.crdt.CrdtStorageClient;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.primitives.CrdtType;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamReducer;
import io.activej.datastream.processor.StreamReducers.BinaryAccumulatorReducer;
import io.activej.datastream.processor.StreamSplitter;
import io.activej.datastream.stats.StreamStats;
import io.activej.datastream.stats.StreamStatsBasic;
import io.activej.datastream.stats.StreamStatsDetailed;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanWithStats;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

@SuppressWarnings("rawtypes") // JMX
public final class CrdtStorageCluster<K extends Comparable<K>, S, P> implements CrdtStorage<K, S>, WithInitializer<CrdtStorageCluster<K, S, P>>, EventloopService, EventloopJmxBeanWithStats {
	private final Eventloop eventloop;
	private final DiscoveryService<P> discoveryService;
	private final CrdtFunction<S> crdtFunction;
	private final Function<P, CrdtStorage<K, S>> provider;
	private final Map<P, CrdtStorage<K, S>> crdtStorages = new LinkedHashMap<>();

	private DiscoveryService.PartitionScheme<P> currentPartitionScheme;
	private boolean stopped;

	// region JMX
	private boolean detailedStats;

	private final StreamStatsBasic<CrdtData<K, S>> uploadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> uploadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<CrdtData<K, S>> downloadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> downloadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<K> removeStats = StreamStats.basic();
	private final StreamStatsDetailed<K> removeStatsDetailed = StreamStats.detailed();
	// endregion

	// region creators
	private CrdtStorageCluster(Eventloop eventloop, DiscoveryService<P> discoveryService, Function<P, CrdtStorage<K, S>> provider, CrdtFunction<S> crdtFunction) {
		this.eventloop = eventloop;
		this.discoveryService = discoveryService;
		this.crdtFunction = crdtFunction;
		this.provider = cachedProvider(provider);
	}

	public static <K extends Comparable<K>, S, P> CrdtStorageCluster<K, S, P> create(Eventloop eventloop,
			DiscoveryService<P> discoveryService, Function<P, CrdtStorage<K, S>> provider,
			CrdtFunction<S> crdtFunction) {
		return new CrdtStorageCluster<>(eventloop, discoveryService, provider, crdtFunction);
	}

	public static <K extends Comparable<K>, S extends CrdtType<S>, P> CrdtStorageCluster<K, S, P> create(Eventloop eventloop,
			DiscoveryService<P> discoveryService, Function<P, CrdtStorage<K, S>> provider
	) {
		return create(eventloop, discoveryService, provider, CrdtFunction.ofCrdtType());
	}

/*
	public CrdtStorageCluster<K, S, P> withReplicationCount(int replicationCount) {
		checkArgument(1 <= replicationCount, "Replication count cannot be less than one");
		this.deadPartitionsThreshold = replicationCount - 1;
		this.replicationCount = replicationCount;
		this.partitions.setTopShards(replicationCount);
		return this;
	}
*/

	// endregion

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public @NotNull Promise<?> start() {
		AsyncSupplier<DiscoveryService.PartitionScheme<P>> discoverySupplier = discoveryService.discover();
		return discoverySupplier.get()
				.then(result -> {
					currentPartitionScheme = result;
					return ping();
				})
				.whenResult(() -> Promises.repeat(() ->
						discoverySupplier.get()
								.map((result, e) -> {
									if (stopped) return false;
									if (e == null) {
										currentPartitionScheme = result;
									}
									return true;
								})
				));
	}

	@Override
	public @NotNull Promise<?> stop() {
		this.stopped = true;
		return Promise.complete();
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		DiscoveryService.PartitionScheme<P> partitionScheme = this.currentPartitionScheme;
		return execute(partitionScheme.getPartitions(), CrdtStorage::upload)
				.then(map -> {
					List<P> alive = new ArrayList<>(map.keySet());
					Sharder<K> sharder = partitionScheme.createSharder(alive);
					if (sharder == null) {
						throw new CrdtException("Incomplete cluster");
					}
					StreamSplitter<CrdtData<K, S>, CrdtData<K, S>> splitter = StreamSplitter.create(
							(item, acceptors) -> {
								int[] selected = sharder.shard(item.getKey());
								//noinspection ForLoopReplaceableByForEach
								for (int i = 0; i < selected.length; i++) {
									acceptors[selected[i]].accept(item);
								}
							});
					for (P pid : alive) {
						splitter.newOutput().streamTo(map.get(pid));
					}
					return Promise.of(splitter.getInput()
							.transformWith(detailedStats ? uploadStatsDetailed : uploadStats));
				});
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp) {
		return execute(currentPartitionScheme.getPartitions(), storage -> storage.download(timestamp))
				.map(map -> {
					StreamReducer<K, CrdtData<K, S>, CrdtData<K, S>> streamReducer = StreamReducer.create();
					for (P pid : map.keySet()) {
						map.get(pid).streamTo(streamReducer.newInput(
								CrdtData::getKey,
								new BinaryAccumulatorReducer<K, CrdtData<K, S>>() {
									@Override
									protected CrdtData<K, S> combine(K key, CrdtData<K, S> nextValue, CrdtData<K, S> accumulator) {
										return new CrdtData<>(key, crdtFunction.merge(accumulator.getState(), nextValue.getState()));
									}
								})
						);
					}
					return streamReducer.getOutput()
							.transformWith(detailedStats ? downloadStatsDetailed : downloadStats);
				});
	}

	@Override
	public Promise<StreamConsumer<K>> remove() {
		DiscoveryService.PartitionScheme<P> partitionScheme = currentPartitionScheme;
		return execute(partitionScheme.getPartitions(), CrdtStorage::remove)
				.map(map -> {
					List<P> alive = new ArrayList<>(map.keySet());
					Sharder<K> sharder = partitionScheme.createSharder(alive);
					if (sharder == null) {
						throw new CrdtException("Incomplete cluster");
					}
					StreamSplitter<K, K> splitter = StreamSplitter.create(
							(item, acceptors) -> {
								int[] selected = sharder.shard(item);
								//noinspection ForLoopReplaceableByForEach
								for (int i = 0; i < selected.length; i++) {
									acceptors[selected[i]].accept(item);
								}
							});
					for (P pid : alive) {
						splitter.newOutput().streamTo(map.get(pid));
					}
					return splitter.getInput()
							.transformWith(detailedStats ? removeStatsDetailed : removeStats);
				});
	}

	public @NotNull Promise<Void> repartition(P sourcePartitionId) {
		DiscoveryService.PartitionScheme<P> partitionScheme = this.currentPartitionScheme;
		CrdtStorage<K, S> source = provider.apply(sourcePartitionId);

		class Tuple {
			private final Try<StreamSupplier<CrdtData<K, S>>> downloader;
			private final Try<StreamConsumer<K>> remover;
			private final Map<P, StreamConsumer<CrdtData<K, S>>> uploaders;

			public Tuple(Try<StreamSupplier<CrdtData<K, S>>> downloader, Try<StreamConsumer<K>> remover, Map<P, StreamConsumer<CrdtData<K, S>>> uploaders) {
				this.downloader = downloader;
				this.remover = remover;
				this.uploaders = uploaders;
			}
		}

		//noinspection Convert2MethodRef - does not compile on Java 8
		return Promises.toTuple((downloader, remover, uploaders) -> new Tuple(downloader, remover, uploaders),
						source.download().toTry(),
						source.remove().toTry(),
						execute(partitionScheme.getPartitions(), CrdtStorage::upload))
				.then(tuple -> {

					if (tuple.uploaders.isEmpty() || tuple.remover.isException() || tuple.downloader.isException()) {
						CrdtException exception = new CrdtException("Repartition exceptions:");
						tuple.downloader.consume(AsyncCloseable::close, exception::addSuppressed);
						tuple.remover.consume(AsyncCloseable::close, exception::addSuppressed);
						tuple.uploaders.values().forEach(AsyncCloseable::close);
						return Promise.ofException(exception);
					}

					List<P> alive = new ArrayList<>(tuple.uploaders.keySet());
					Sharder<K> sharder = partitionScheme.createSharder(alive);
					if (sharder == null) {
						throw new CrdtException("Incomplete cluster");
					}
					int sourceIdx = alive.indexOf(sourcePartitionId);
					StreamSplitter<CrdtData<K, S>, ?> splitter = StreamSplitter.create(
							(item, acceptors) -> {
								boolean sourceInShards = false;
								int[] shards = sharder.shard(item.getKey());
								//noinspection ForLoopReplaceableByForEach
								for (int i = 0; i < shards.length; i++) {
									int idx = shards[i];
									if (sourceIdx == idx) {
										sourceInShards = true;
									} else {
										acceptors[idx].accept(item);
									}
								}
								if (!sourceInShards) {
									acceptors[acceptors.length - 1].accept(item.getKey());
								}
							});

					StreamConsumer<CrdtData<K, S>> uploader = splitter.getInput()
							.transformWith(detailedStats ? uploadStatsDetailed : uploadStats);
					StreamConsumer<K> remover = tuple.remover.get()
							.transformWith(detailedStats ? removeStatsDetailed : removeStats);
					StreamSupplier<CrdtData<K, S>> downloader = tuple.downloader.get()
							.transformWith(detailedStats ? downloadStatsDetailed : downloadStats);

					for (P pid : alive) {
						//noinspection unchecked
						((StreamSupplier<CrdtData<K, S>>) splitter.newOutput())
								.streamTo(tuple.uploaders.get(pid));
					}

					SettablePromise<Void> removerEos = new SettablePromise<>();
					//noinspection unchecked
					((StreamSupplier<K>) splitter.newOutput())
							.withEndOfStream(eos -> removerEos)
							.streamTo(remover.withAcknowledgement(ack -> downloader.getEndOfStream()));

					return downloader.streamTo(uploader)
							.whenResult(() -> removerEos.set(null))
							.then(remover::getAcknowledgement);
				});
	}

	@Override
	public Promise<Void> ping() {
		DiscoveryService.PartitionScheme<P> partitionScheme = this.currentPartitionScheme;
		return execute(partitionScheme.getPartitions(), CrdtStorage::ping)
				.whenResult(map -> {
					Sharder<K> sharder = partitionScheme.createSharder(new ArrayList<>(map.keySet()));
					if (sharder == null) {
						throw new CrdtException("Incomplete cluster");
					}
				})
				.toVoid();
	}

	@NotNull
	private <T> Promise<Map<P, T>> execute(Set<P> partitions, AsyncFunction<CrdtStorage<K, S>, T> method) {
		Map<P, T> map = new HashMap<>();
		return Promises.all(
						partitions.stream()
								.map(partitionId -> method.apply(provider.apply(partitionId))
										.map((t, e) -> e == null ? map.put(partitionId, t) : null)
								))
				.map($ -> map);
	}

	private Function<P, CrdtStorage<K, S>> cachedProvider(Function<P, CrdtStorage<K, S>> function) {
		return id -> crdtStorages.computeIfAbsent(id, function);
	}

	// region JMX
/*
	@JmxAttribute
	public int getDeadPartitionsThreshold() {
		return deadPartitionsThreshold;
	}

	@JmxAttribute
	public int getReplicationCount() {
		return replicationCount;
	}

	@JmxOperation
	public void setPersistenceOptions(int deadPartitionsThreshold, int uploadTargets) {
		withPersistenceOptions(deadPartitionsThreshold, uploadTargets);
	}

	@JmxAttribute
	public void setReplicationCount(int replicationCount) {
		withReplicationCount(replicationCount);
	}

	@JmxAttribute(name = "")
	public CrdtPartitions getPartitionsJmx() {
		return partitions;
	}
*/

	@JmxAttribute
	public boolean isDetailedStats() {
		return detailedStats;
	}

	@JmxOperation
	public void startDetailedMonitoring() {
		detailedStats = true;
	}

	@JmxOperation
	public void stopDetailedMonitoring() {
		detailedStats = false;
	}

	@JmxAttribute
	public StreamStatsBasic getUploadStats() {
		return uploadStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getUploadStatsDetailed() {
		return uploadStatsDetailed;
	}

	@JmxAttribute
	public StreamStatsBasic getDownloadStats() {
		return downloadStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getDownloadStatsDetailed() {
		return downloadStatsDetailed;
	}

	@JmxAttribute
	public StreamStatsBasic getRemoveStats() {
		return removeStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getRemoveStatsDetailed() {
		return removeStatsDetailed;
	}

	@JmxAttribute
	public Map<P, CrdtStorageClient> getCrdtStorageClients() {
		return crdtStorages.entrySet().stream()
				.filter(entry -> entry.getValue() instanceof CrdtStorageClient)
				.collect(toMap(Map.Entry::getKey, e -> (CrdtStorageClient) e.getValue()));
	}
	// endregion
}
