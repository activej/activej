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

package io.activej.crdt.storage.local;

import io.activej.async.function.AsyncRunnable;
import io.activej.async.function.AsyncRunnables;
import io.activej.async.function.AsyncSupplier;
import io.activej.async.service.EventloopService;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.Utils;
import io.activej.common.initializer.WithInitializer;
import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtException;
import io.activej.crdt.function.CrdtFilter;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.primitives.CrdtType;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelConsumers;
import io.activej.csp.ChannelSupplier;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.processor.StreamFilter;
import io.activej.datastream.processor.StreamReducer;
import io.activej.datastream.processor.StreamReducers;
import io.activej.datastream.stats.StreamStats;
import io.activej.datastream.stats.StreamStatsBasic;
import io.activej.datastream.stats.StreamStatsDetailed;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanWithStats;
import io.activej.fs.ActiveFs;
import io.activej.fs.FileMetadata;
import io.activej.fs.exception.FileNotFoundException;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.activej.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.activej.csp.ChannelSupplier.ofPromise;
import static io.activej.datastream.processor.StreamFilter.mapper;
import static io.activej.fs.ActiveFsAdapters.subdirectory;
import static io.activej.promise.Promises.all;
import static io.activej.promise.Promises.toTuple;

@SuppressWarnings("rawtypes")
public final class CrdtStorageFs<K extends Comparable<K>, S> implements CrdtStorage<K, S>,
		WithInitializer<CrdtStorageFs<K, S>>, EventloopService, EventloopJmxBeanWithStats {
	private static final Logger logger = LoggerFactory.getLogger(CrdtStorageFs.class);

	private final Eventloop eventloop;
	private final ActiveFs fs;
	private final CrdtFunction<S> function;
	private final CrdtDataSerializer<K, S> serializer;

	private UnaryOperator<String> namingStrategy = ext -> UUID.randomUUID() + "." + ext;

	private ActiveFs tombstoneFolderFs;
	private CrdtFilter<S> filter = $ -> true;

	// region JMX
	private boolean detailedStats;

	private final AsyncRunnable consolidate = AsyncRunnables.reuse(this::doConsolidate);

	private final StreamStatsBasic<CrdtData<K, S>> uploadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> uploadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<CrdtData<K, S>> downloadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> downloadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<K> removeStats = StreamStats.basic();
	private final StreamStatsDetailed<K> removeStatsDetailed = StreamStats.detailed();

	private final PromiseStats consolidationStats = PromiseStats.create(Duration.ofMinutes(5));
	// endregion

	// region creators
	private CrdtStorageFs(
			Eventloop eventloop,
			ActiveFs fs,
			ActiveFs tombstoneFolderFs, CrdtDataSerializer<K, S> serializer, CrdtFunction<S> function
	) {
		this.eventloop = eventloop;
		this.fs = fs;
		this.function = function;
		this.serializer = serializer;
		this.tombstoneFolderFs = tombstoneFolderFs;
	}

	public static <K extends Comparable<K>, S> CrdtStorageFs<K, S> create(
			Eventloop eventloop, ActiveFs fs,
			CrdtDataSerializer<K, S> serializer,
			CrdtFunction<S> function
	) {
		return new CrdtStorageFs<>(eventloop, fs, subdirectory(fs, ".tombstones"), serializer, function);
	}

	public static <K extends Comparable<K>, S extends CrdtType<S>> CrdtStorageFs<K, S> create(
			Eventloop eventloop, ActiveFs fs,
			CrdtDataSerializer<K, S> serializer
	) {
		return new CrdtStorageFs<>(eventloop, fs, subdirectory(fs, ".tombstones"), serializer, CrdtFunction.ofCrdtType());
	}

	public CrdtStorageFs<K, S> withNamingStrategy(UnaryOperator<String> namingStrategy) {
		this.namingStrategy = namingStrategy;
		return this;
	}

	public CrdtStorageFs<K, S> withTombstoneFolder(String subdirectory) {
		tombstoneFolderFs = subdirectory(fs, subdirectory);
		return this;
	}

	public CrdtStorageFs<K, S> withFilter(CrdtFilter<S> filter) {
		this.filter = filter;
		return this;
	}

	public CrdtStorageFs<K, S> withTombstoneFolderClient(ActiveFs tombstoneFolderFs) {
		this.tombstoneFolderFs = tombstoneFolderFs;
		return this;
	}
	// endregion

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		return Promise.of(this.<CrdtData<K, S>>uploadNonEmpty(() ->
								fs.upload(namingStrategy.apply("bin"))
										.mapException(e1 -> new CrdtException("Failed to upload CRDT data to file", e1)),
						supplier -> supplier
								.transformWith(detailedStats ? uploadStatsDetailed : uploadStats)
								.transformWith(ChannelSerializer.create(serializer)))
				.withAcknowledgement(ack -> ack
						.mapException(e -> new CrdtException("Error while uploading CRDT data to file", e))));
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp) {
		return Promises.retry(($, e) -> !(e instanceof FileNotFoundException),
						() -> toTuple(fs.list("*"), tombstoneFolderFs.list("*"))
								.then(tuple -> doDownload(tuple.getValue1(), tuple.getValue2(), timestamp))
								.map(supplier -> supplier.transformWith(detailedStats ? downloadStatsDetailed : downloadStats)))
				.mapException(e -> new CrdtException("Failed to download CRDT data", e));
	}

	private Promise<StreamSupplier<CrdtData<K, S>>> doDownload(Map<String, FileMetadata> files, Map<String, FileMetadata> tombstones, long timestamp) {
		return Promises.toList(files.entrySet()
						.stream()
						.filter(e -> timestamp == 0 || e.getValue().getTimestamp() >= timestamp)
						.map(entry -> fs.download(entry.getKey())
								.map(supplier -> supplier
										.transformWith(ChannelDeserializer.create(serializer))
										.transformWith(mapper(data -> {
											S partial = function.extract(data.getState(), timestamp);
											return partial != null ? new CrdtReducingData<>(data.getKey(), partial, entry.getValue().getTimestamp()) : null;
										}))
										.transformWith(StreamFilter.create(Objects::nonNull)))))
				.combine(Promises.toList(tombstones.entrySet()
								.stream()
								.filter(e -> timestamp == 0 || e.getValue().getTimestamp() >= timestamp)
								.map(entry -> tombstoneFolderFs.download(entry.getKey())
										.map(supplier -> supplier
												.transformWith(ChannelDeserializer.create(serializer.getKeySerializer()))
												.transformWith(mapper(key -> new CrdtReducingData<>(key, (S) null, entry.getValue().getTimestamp())))))),
						Utils::concat)
				.map(suppliers -> {
					StreamReducer<K, CrdtData<K, S>, CrdtAccumulator<S>> reducer = StreamReducer.create();

					suppliers.forEach(supplier -> supplier.streamTo(reducer.newInput(x -> x.key, new CrdtReducer())));

					return reducer.getOutput()
							.withEndOfStream(eos -> eos
									.mapException(e -> new CrdtException("Error while downloading CRDT data", e)));

				});
	}

	@Override
	public Promise<StreamConsumer<K>> remove() {
		return Promise.of(this.<K>uploadNonEmpty(() ->
								tombstoneFolderFs.upload(namingStrategy.apply("tomb"))
										.mapException(e -> new CrdtException("Failed to remove CRDT data", e)),
						supplier -> supplier
								.transformWith(detailedStats ? removeStatsDetailed : removeStats)
								.transformWith(ChannelSerializer.create(serializer.getKeySerializer()))
				)
				.withAcknowledgement(ack -> ack
						.mapException(e -> new CrdtException("Error while removing CRDT data", e))));
	}

	@Override
	public Promise<Void> ping() {
		return fs.ping()
				.mapException(e -> new CrdtException("Failed to PING file system", e));
	}

	@Override
	public @NotNull Promise<Void> start() {
		return Promise.complete();
	}

	@Override
	public @NotNull Promise<Void> stop() {
		return Promise.complete();
	}

	public Promise<Void> consolidate() {
		return consolidate.run()
				.whenComplete(consolidationStats.recordStats());
	}

	private Promise<Void> doConsolidate() {
		return consolidateFiles()
				.then(this::consolidateTombstones);
	}

	private Promise<Void> consolidateFiles() {
		return fs.list("*")
				.map(CrdtStorageFs::pickFilesForConsolidation)
				.then(filesMap -> {
					if (filesMap.isEmpty()) {
						logger.info("No files to consolidate");
						return Promise.complete();
					}

					String name = namingStrategy.apply("bin");
					Set<String> filesToConsolidate = filesMap.keySet();

					logger.info("Started consolidating files into {} from {}", name, filesToConsolidate);

					return tombstoneFolderFs.list("*")
							.then(tombstoneMap -> doDownload(filesMap, tombstoneMap, 0))
							.then(crdtSupplier -> crdtSupplier.streamTo(uploadNonEmpty(() -> fs.upload(name),
									supplier -> supplier.transformWith(ChannelSerializer.create(serializer)))))
							.then(() -> fs.deleteAll(filesToConsolidate));
				})
				.mapException(e -> new CrdtException("Files consolidation failed", e));
	}

	private Promise<Void> consolidateTombstones() {
		return tombstoneFolderFs.list("*")
				.map(CrdtStorageFs::pickFilesForConsolidation)
				.then(tombstoneMap -> {
					if (tombstoneMap.isEmpty()) {
						logger.info("No tombstones to consolidate");
						return Promise.complete();
					}

					StreamReducer<K, K, Void> reducer = StreamReducer.create();

					String name = namingStrategy.apply("tomb");
					Set<String> tombstonesToConsolidate = tombstoneMap.keySet();

					logger.info("Started consolidating tombstones into {} from {}", name, tombstonesToConsolidate);

					tombstonesToConsolidate.forEach(tombstone -> ofPromise(tombstoneFolderFs.download(tombstone))
							.transformWith(ChannelDeserializer.create(serializer.getKeySerializer()))
							.streamTo(reducer.newInput(Function.identity(), StreamReducers.deduplicateReducer())));

					return reducer.getOutput()
							.transformWith(ChannelSerializer.create(serializer.getKeySerializer()))
							.streamTo(tombstoneFolderFs.upload(name))
							.then(() -> tombstoneFolderFs.deleteAll(tombstonesToConsolidate));
				})
				.mapException(e -> new CrdtException("Tombstones consolidation failed", e));
	}

	@VisibleForTesting
	static Map<String, FileMetadata> pickFilesForConsolidation(Map<String, FileMetadata> files) {
		if (files.isEmpty()) return files;

		Map<Integer, List<Map.Entry<String, FileMetadata>>> groups = new TreeMap<>();
		for (Map.Entry<String, FileMetadata> entry : files.entrySet()) {
			int groupIdx = (int) Math.log10(entry.getValue().getSize());
			groups.computeIfAbsent(groupIdx, k -> new ArrayList<>()).add(entry);
		}

		List<Map.Entry<String, FileMetadata>> groupToConsolidate = Collections.emptyList();

		for (List<Map.Entry<String, FileMetadata>> group : groups.values()) {
			int groupSize = group.size();
			if (groupSize > 1 && groupSize > groupToConsolidate.size()) {
				groupToConsolidate = group;
			}
		}

		return Utils.entriesToMap(groupToConsolidate.stream());
	}

	private <T> StreamConsumer<T> uploadNonEmpty(AsyncSupplier<ChannelConsumer<ByteBuf>> consumerFn, Function<StreamSupplier<T>, ChannelSupplier<ByteBuf>> mapping) {
		SettablePromise<ChannelConsumer<ByteBuf>> consumerPromise = new SettablePromise<>();
		NonEmptyFilter<T> nonEmptyFilter = new NonEmptyFilter<>(() -> consumerFn.get()
				.whenComplete(consumerPromise::accept));

		return StreamConsumer.ofSupplier(supplier -> mapping.apply(supplier.transformWith(nonEmptyFilter))
				.withEndOfStream(eos -> eos
						.whenComplete(() -> {
							if (nonEmptyFilter.isEmpty()) {
								consumerPromise.set(ChannelConsumers.recycling());
							}
						}))
				.streamTo(consumerPromise));
	}

	static class CrdtReducingData<K extends Comparable<K>, S> {
		final K key;
		final @Nullable S state;
		final long timestamp;

		CrdtReducingData(K key, @Nullable S state, long timestamp) {
			this.key = key;
			this.state = state;
			this.timestamp = timestamp;
		}
	}

	static class CrdtAccumulator<S> {
		@Nullable S state;
		long maxAppendTimestamp;
		long maxRemoveTimestamp;

		CrdtAccumulator(@Nullable S state, long maxAppendTimestamp, long maxRemoveTimestamp) {
			this.state = state;
			this.maxAppendTimestamp = maxAppendTimestamp;
			this.maxRemoveTimestamp = maxRemoveTimestamp;
		}
	}

	class CrdtReducer implements StreamReducers.Reducer<K, CrdtReducingData<K, S>, CrdtData<K, S>, CrdtAccumulator<S>> {
		@Override
		public CrdtAccumulator<S> onFirstItem(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtReducingData<K, S> firstValue) {
			if (firstValue.state != null) {
				return new CrdtAccumulator<>(firstValue.state, firstValue.timestamp, 0);
			}
			return new CrdtAccumulator<>(null, 0, firstValue.timestamp);
		}

		@Override
		public CrdtAccumulator<S> onNextItem(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtReducingData<K, S> nextValue, CrdtAccumulator<S> accumulator) {
			if (nextValue.state != null) {
				accumulator.state = accumulator.state != null ? function.merge(accumulator.state, nextValue.state) : nextValue.state;
				if (nextValue.timestamp > accumulator.maxAppendTimestamp) {
					accumulator.maxAppendTimestamp = nextValue.timestamp;
				}
			} else if (nextValue.timestamp > accumulator.maxRemoveTimestamp) {
				accumulator.maxRemoveTimestamp = nextValue.timestamp;
			}
			return accumulator;
		}

		@Override
		public void onComplete(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtAccumulator<S> accumulator) {
			if (accumulator.state != null
					&& accumulator.maxRemoveTimestamp < accumulator.maxAppendTimestamp
					&& filter.test(accumulator.state)) {
				stream.accept(new CrdtData<>(key, accumulator.state));
			}
		}
	}

	private static final class NonEmptyFilter<T> extends StreamFilter<T, T> {
		private final Runnable onNonEmpty;
		private boolean empty = true;

		private NonEmptyFilter(Runnable onNonEmpty) {
			this.onNonEmpty = onNonEmpty;
		}

		@Override
		protected @NotNull StreamDataAcceptor<T> onResumed(@NotNull StreamDataAcceptor<T> output) {
			return item -> {
				if (empty) {
					empty = false;
					onNonEmpty.run();
				}
				output.accept(item);
			};
		}

		public boolean isEmpty() {
			return empty;
		}
	}

	// region JMX
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
	public PromiseStats getConsolidationStats() {
		return consolidationStats;
	}
	// endregion
}
