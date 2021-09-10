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

import io.activej.async.process.AsyncCloseable;
import io.activej.async.service.EventloopService;
import io.activej.common.collection.Try;
import io.activej.common.initializer.WithInitializer;
import io.activej.common.ref.RefInt;
import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtException;
import io.activej.crdt.function.CrdtFilter;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.primitives.CrdtType;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.util.RendezvousHashSharder;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamReducer;
import io.activej.datastream.processor.StreamReducers.BinaryAccumulatorReducer;
import io.activej.datastream.processor.StreamReducers.Reducer;
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
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.activej.common.Checks.checkArgument;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("rawtypes") // JMX
public final class CrdtStorageCluster<K extends Comparable<K>, S, P extends Comparable<P>> implements CrdtStorage<K, S>, WithInitializer<CrdtStorageCluster<K, S, P>>, EventloopService, EventloopJmxBeanWithStats {
	private final CrdtPartitions<K, S, P> partitions;

	private final CrdtFunction<S> function;

	/**
	 * Maximum allowed number of dead partitions, if there are more dead partitions than this number,
	 * the cluster is considered malformed.
	 */
	private int deadPartitionsThreshold = 0;

	/**
	 * Number of uploads that are initiated.
	 */
	private int replicationCount = 1;

	private CrdtFilter<S> filter;

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
	private CrdtStorageCluster(CrdtPartitions<K, S, P> partitions, CrdtFunction<S> function) {
		this.partitions = partitions;
		this.function = function;
	}

	public static <K extends Comparable<K>, S, P extends Comparable<P>> CrdtStorageCluster<K, S, P> create(
			CrdtPartitions<K, S, P> partitions, CrdtFunction<S> crdtFunction
	) {
		return new CrdtStorageCluster<>(partitions, crdtFunction);
	}

	public static <K extends Comparable<K>, S extends CrdtType<S>, P extends Comparable<P>> CrdtStorageCluster<K, S, P> create(
			CrdtPartitions<K, S, P> partitions
	) {
		return new CrdtStorageCluster<>(partitions, CrdtFunction.ofCrdtType());
	}

	public CrdtStorageCluster<K, S, P> withReplicationCount(int replicationCount) {
		checkArgument(1 <= replicationCount, "Replication count cannot be less than one");
		this.deadPartitionsThreshold = replicationCount - 1;
		this.replicationCount = replicationCount;
		this.partitions.setTopShards(replicationCount);
		return this;
	}

	public CrdtStorageCluster<K, S, P> withFilter(CrdtFilter<S> filter) {
		this.filter = filter;
		return this;
	}

	/**
	 * Sets the replication count as well as number of upload targets that determines the number of servers where CRDT data will be uploaded.
	 */
	@SuppressWarnings("UnusedReturnValue")
	public CrdtStorageCluster<K, S, P> withPersistenceOptions(int deadPartitionsThreshold, int uploadTargets) {
		checkArgument(0 <= deadPartitionsThreshold && deadPartitionsThreshold < partitions.getPartitions().size(),
				"Dead partitions threshold cannot be less than zero or greater than number of partitions");
		checkArgument(0 <= uploadTargets,
				"Number of upload targets should not be less than zero");
		checkArgument(uploadTargets <= partitions.getPartitions().size(),
				"Number of upload targets should not exceed total number of partitions");
		this.deadPartitionsThreshold = deadPartitionsThreshold;
		this.replicationCount = uploadTargets;
		this.partitions.setTopShards(uploadTargets);
		return this;
	}
	// endregion

	@Override
	public @NotNull Eventloop getEventloop() {
		return partitions.getEventloop();
	}

	public CrdtPartitions<K, S, P> getPartitions() {
		return partitions;
	}

	private <T extends AsyncCloseable> Promise<List<Container<T>>> connect(Function<CrdtStorage<K, S>, Promise<T>> method) {
		return Promises.toList(
						partitions.getAlivePartitions().entrySet().stream()
								.map(entry ->
										method.apply(entry.getValue())
												.map(t -> new Container<>(entry.getKey(), t))
												.whenException(err -> partitions.markDead(entry.getKey(), err))
												.toTry()))
				.map(this::checkStillNotDead)
				.map(tries -> {
					List<Container<T>> successes = tries.stream()
							.filter(Try::isSuccess)
							.map(Try::get)
							.collect(toList());
					if (successes.isEmpty()) {
						throw new CrdtException("No successful connections");
					}
					return successes;
				});
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		return connect(CrdtStorage::upload)
				.then(containers -> {
					RendezvousHashSharder<P> sharder = partitions.getSharder();
					StreamSplitter<CrdtData<K, S>, CrdtData<K, S>> splitter = StreamSplitter.create(
							(item, acceptors) -> {
								for (int index : sharder.shard(item.getKey())) {
									acceptors[index].accept(item);
								}
							});
					RefInt failedRef = tolerantSplit(containers, sharder, splitter);
					return Promise.of(splitter.getInput()
							.transformWith(detailedStats ? uploadStatsDetailed : uploadStats)
							.withAcknowledgement(ack -> ack
									.mapException(e -> new CrdtException("Cluster 'upload' failed", e))
									.whenResult(() -> {
												if (containers.size() - failedRef.value < replicationCount) {
													throw new CrdtException("Failed to upload data to the required number of partitions");
												}
											}
									)));
				});
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp) {
		return connect(storage -> storage.download(timestamp))
				.map(containers -> {
					StreamReducer<K, CrdtData<K, S>, CrdtData<K, S>> streamReducer = StreamReducer.create();
					RefInt failedRef = tolerantReduce(containers, streamReducer);
					return streamReducer.getOutput()
							.transformWith(detailedStats ? downloadStatsDetailed : downloadStats)
							.withEndOfStream(eos -> eos
									.mapException(e -> new CrdtException("Cluster 'download' failed", e))
									.whenResult(() -> {
										int deadBeforeDownload = partitions.getPartitions().size() - containers.size();
										if (deadBeforeDownload + failedRef.get() >= replicationCount) {
											throw new CrdtException("Failed to download from the required number of partitions");
										}
									}));
				});
	}

	@Override
	public Promise<StreamConsumer<K>> remove() {
		return connect(CrdtStorage::remove)
				.map(containers -> {
					RendezvousHashSharder<P> sharderAll = RendezvousHashSharder.create(containers.stream()
							.map(container -> container.id)
							.collect(Collectors.toSet()), containers.size());
					StreamSplitter<K, K> splitter = StreamSplitter.create((item, acceptors) -> {
						for (int index : sharderAll.shard(item)) {
							acceptors[index].accept(item);
						}
					});
					RefInt failedRef = tolerantSplit(containers, sharderAll, splitter);
					return splitter.getInput()
							.transformWith(detailedStats ? removeStatsDetailed : removeStats)
							.withAcknowledgement(ack -> ack
									.mapException(e -> new CrdtException("Cluster 'remove' failed", e))
									.whenResult(() -> {
										if (partitions.getPartitions().size() - containers.size() + failedRef.get() != 0) {
											throw new CrdtException("Failed to remove items from all partitions");
										}
									}));
				});
	}

	@Override
	public Promise<Void> ping() {
		return Promise.complete();  // Promises.all(aliveClients.values().stream().map(CrdtClient::ping));
	}

	@Override
	public @NotNull Promise<Void> start() {
		return Promise.complete();
	}

	@Override
	public @NotNull Promise<Void> stop() {
		return Promise.complete();
	}

	private <T extends AsyncCloseable> List<Try<Container<T>>> checkStillNotDead(List<Try<Container<T>>> value) throws CrdtException {
		Map<P, CrdtStorage<K, S>> deadPartitions = partitions.getDeadPartitions();
		if (deadPartitions.size() > deadPartitionsThreshold) {
			CrdtException exception = new CrdtException("There are more dead partitions than allowed(" +
					deadPartitions.size() + " dead, threshold is " + deadPartitionsThreshold + "), aborting");
			value.stream()
					.filter(Try::isSuccess)
					.map(Try::get)
					.forEach(container -> container.value.closeEx(exception));
			throw exception;
		}
		return value;
	}

	private <T> RefInt tolerantSplit(
			List<Container<StreamConsumer<T>>> containers,
			RendezvousHashSharder<P> sharder,
			StreamSplitter<T, T> splitter
	) {
		RefInt failed = new RefInt(0);
		for (Container<StreamConsumer<T>> container : containers) {
			StreamSupplier<T> supplier = splitter.addOutput(splitter.new Output() {
				@Override
				protected void onError(Exception e) {
					partitions.markDead(container.id, e);
					sharder.recompute(partitions.getAlivePartitions().keySet());
					if (++failed.value == containers.size()) {
						splitter.getInput().closeEx(e);
					} else {
						complete();
						sync();
					}
				}

				@Override
				protected boolean canProceed() {
					return isReady() || isComplete();
				}
			});
			supplier.streamTo(container.value);
		}
		return failed;
	}

	private RefInt tolerantReduce(
			List<Container<StreamSupplier<CrdtData<K, S>>>> containers,
			StreamReducer<K, CrdtData<K, S>, CrdtData<K, S>> streamReducer
	) {
		RefInt failed = new RefInt(0);
		for (Container<StreamSupplier<CrdtData<K, S>>> container : containers) {
			Reducer<K, CrdtData<K, S>, CrdtData<K, S>, CrdtData<K, S>> reducer = filter == null ?
					new BinaryAccumulatorReducer<>((a, b) -> new CrdtData<>(a.getKey(), function.merge(a.getState(), b.getState()))) :
					new BinaryAccumulatorReducer<K, CrdtData<K, S>>((a, b) -> new CrdtData<>(a.getKey(), function.merge(a.getState(), b.getState()))) {
						@Override
						protected boolean filter(CrdtData<K, S> value) {
							return filter.test(value.getState());
						}
					};

			container.value.streamTo(streamReducer.addInput(
					streamReducer.new SimpleInput<CrdtData<K, S>>(CrdtData::getKey, reducer) {
						boolean awaiting;

						@Override
						protected void onError(Exception e) {
							if (awaiting) {
								advance();
							}
							closeInput();
							partitions.markDead(container.id, e);
							if (++failed.value == containers.size()) {
								super.onError(e);
							} else {
								continueReduce();
							}
						}

						@Override
						protected int await() {
							assert !awaiting;
							awaiting = true;
							return super.await();
						}

						@Override
						protected int advance() {
							assert awaiting;
							awaiting = false;
							return super.advance();
						}
					}));
		}
		return failed;
	}

	// region JMX
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
	// endregion

	private final class Container<T extends AsyncCloseable> {
		final P id;
		final T value;

		Container(P id, T value) {
			this.id = id;
			this.value = value;
		}
	}
}
