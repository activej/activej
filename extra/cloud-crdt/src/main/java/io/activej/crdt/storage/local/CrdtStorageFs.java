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

import io.activej.async.service.EventloopService;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.api.WithInitializer;
import io.activej.crdt.CrdtData;
import io.activej.crdt.function.CrdtFilter;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.primitives.CrdtType;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.csp.ChannelConsumer;
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
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.fs.ActiveFs;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.activej.crdt.util.Utils.wrapException;
import static io.activej.fs.ActiveFsAdapters.subdirectory;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("rawtypes")
public final class CrdtStorageFs<K extends Comparable<K>, S> implements CrdtStorage<K, S>,
		WithInitializer<CrdtStorageFs<K, S>>, EventloopService, EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(CrdtStorageFs.class);

	private final Eventloop eventloop;
	private final ActiveFs fs;
	private final CrdtFunction<S> function;
	private final CrdtDataSerializer<K, S> serializer;

	private Function<String, String> namingStrategy = ext -> UUID.randomUUID().toString() + "." + ext;
	private Duration consolidationMargin = Duration.ofMinutes(30);

	private ActiveFs consolidationFolderFs;
	private ActiveFs tombstoneFolderFs;
	private CrdtFilter<S> filter = $ -> true;

	// region JMX
	private boolean detailedStats;

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
			ActiveFs consolidationFolderFs, ActiveFs tombstoneFolderFs, CrdtDataSerializer<K, S> serializer, CrdtFunction<S> function
	) {
		this.eventloop = eventloop;
		this.fs = fs;
		this.function = function;
		this.serializer = serializer;
		this.consolidationFolderFs = consolidationFolderFs;
		this.tombstoneFolderFs = tombstoneFolderFs;
	}

	public static <K extends Comparable<K>, S> CrdtStorageFs<K, S> create(
			Eventloop eventloop, ActiveFs fs,
			CrdtDataSerializer<K, S> serializer,
			CrdtFunction<S> function
	) {
		return new CrdtStorageFs<>(eventloop, fs, subdirectory(fs, ".consolidation"), subdirectory(fs, ".tombstones"), serializer, function);
	}

	public static <K extends Comparable<K>, S extends CrdtType<S>> CrdtStorageFs<K, S> create(
			Eventloop eventloop, ActiveFs fs,
			CrdtDataSerializer<K, S> serializer
	) {
		return new CrdtStorageFs<>(eventloop, fs, subdirectory(fs, ".consolidation"), subdirectory(fs, ".tombstones"), serializer, CrdtFunction.ofCrdtType());
	}

	@SuppressWarnings("UnusedReturnValue")
	public CrdtStorageFs<K, S> withConsolidationMargin(Duration consolidationMargin) {
		this.consolidationMargin = consolidationMargin;
		return this;
	}

	public CrdtStorageFs<K, S> withNamingStrategy(Function<String, String> namingStrategy) {
		this.namingStrategy = namingStrategy;
		return this;
	}

	public CrdtStorageFs<K, S> withConsolidationFolder(String subdirectory) {
		consolidationFolderFs = subdirectory(fs, subdirectory);
		return this;
	}

	public CrdtStorageFs<K, S> withTombstoneFolder(String subdirectory) {
		tombstoneFolderFs = subdirectory(fs, subdirectory);
		return this;
	}

	public CrdtStorageFs<K, S> withConsolidationFolderClient(ActiveFs consolidationFolderFs) {
		this.consolidationFolderFs = consolidationFolderFs;
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

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		return fs.upload(namingStrategy.apply("bin"))
				.map(consumer -> StreamConsumer.<CrdtData<K, S>>ofSupplier(supplier -> supplier
						.transformWith(detailedStats ? uploadStatsDetailed : uploadStats)
						.transformWith(ChannelSerializer.create(serializer))
						.streamTo(consumer))
						.withAcknowledgement(ack -> ack
								.thenEx(wrapException(() -> "Error while uploading CRDT data to file"))))
				.thenEx(wrapException(() -> "Failed to upload CRDT data to file"));
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp) {
		return Promises.toTuple(fs.list("*"), tombstoneFolderFs.list("*"))
				.map(f -> {
					StreamReducer<K, CrdtData<K, S>, CrdtAccumulator<S>> reducer = StreamReducer.create();

					Stream<Promise<Void>> files = f.getValue1().entrySet()
							.stream()
							.filter(e -> timestamp == 0 || e.getValue().getTimestamp() >= timestamp)
							.map(entry -> ChannelSupplier.ofPromise(fs.download(entry.getKey()))
									.transformWith(ChannelDeserializer.create(serializer))
									.transformWith(StreamFilter.mapper(data -> {
										S partial = function.extract(data.getState(), timestamp);
										return partial != null ? new CrdtReducingData<>(data.getKey(), partial, entry.getValue().getTimestamp()) : null;
									}))
									.transformWith(StreamFilter.create(Objects::nonNull))
									.streamTo(reducer.newInput(x -> x.key, new CrdtReducer())));

					Stream<Promise<Void>> tombstones = f.getValue2().entrySet()
							.stream()
							.filter(e -> timestamp == 0 || e.getValue().getTimestamp() >= timestamp)
							.map(entry -> ChannelSupplier.ofPromise(tombstoneFolderFs.download(entry.getKey()))
									.transformWith(ChannelDeserializer.create(serializer.getKeySerializer()))
									.transformWith(StreamFilter.mapper(key -> new CrdtReducingData<>(key, (S) null, entry.getValue().getTimestamp())))
									.streamTo(reducer.newInput(x -> x.key, new CrdtReducer())));

					//noinspection ResultOfMethodCallIgnored
					Promises.all(Stream.concat(files, tombstones));

					return reducer.getOutput()
							.transformWith(detailedStats ? downloadStatsDetailed : downloadStats)
							.withEndOfStream(eos -> eos
									.thenEx(wrapException(() -> "Error while downloading CRDT data")));
				})
				.thenEx(wrapException(() -> "Failed to download CRDT data"));
	}

	@Override
	public Promise<StreamConsumer<K>> remove() {
		return tombstoneFolderFs.upload(namingStrategy.apply("tomb"))
				.map(consumer -> StreamConsumer.<K>ofSupplier(supplier -> supplier
						.transformWith(detailedStats ? removeStatsDetailed : removeStats)
						.transformWith(ChannelSerializer.create(serializer.getKeySerializer()))
						.streamTo(consumer))
						.withAcknowledgement(ack -> ack
								.thenEx(wrapException(() -> "Error while removing CRDT data"))))
				.thenEx(wrapException(() -> "Failed to remove CRDT data"));
	}

	@Override
	public Promise<Void> ping() {
		return fs.ping()
				.thenEx(wrapException(() -> "Failed to PING file system"));
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

	public Promise<Void> consolidate() {
		long barrier = eventloop.currentInstant().minus(consolidationMargin).toEpochMilli();
		Set<String> blacklist = new HashSet<>();

		return consolidationFolderFs.list("*")
				.then(list ->
						Promises.all(list.entrySet().stream()
								.filter(entry -> entry.getValue().getTimestamp() > barrier)
								.map(entry -> ChannelSupplier.ofPromise(fs.download(entry.getKey()))
										.toCollector(ByteBufs.collector())
										.whenResult(byteBuf -> blacklist.addAll(Arrays.asList(byteBuf.asString(UTF_8).split("\n"))))
										.toVoid())))
				.then(() -> fs.list("*"))
				.then(list -> {
					String name = namingStrategy.apply("bin");
					List<String> files = list.keySet().stream()
							.filter(fileName -> !blacklist.contains(fileName))
							.collect(toList());
					String dump = String.join("\n", files);

					logger.info("started consolidating into {} from {}", name, files);

					String metafile = namingStrategy.apply("dump");
					//noinspection Convert2MethodRef
					return consolidationFolderFs.upload(metafile)
							.then(consumer ->
									ChannelSupplier.of(ByteBuf.wrapForReading(dump.getBytes(UTF_8)))
											.streamTo(consumer))
							.then(() -> download())
							.then(producer -> producer
									.transformWith(ChannelSerializer.create(serializer))
									.streamTo(ChannelConsumer.ofPromise(fs.upload(name))))
							.then(() -> tombstoneFolderFs.list("*")
									.map(fileMap -> tombstoneFolderFs.deleteAll(fileMap.keySet()))
							)
							.then(() -> consolidationFolderFs.delete(metafile))
							.then(() -> fs.deleteAll(new HashSet<>(files)));
				})
				.thenEx(wrapException(() -> "Consolidation failed"))
				.whenComplete(consolidationStats.recordStats());
	}

	static class CrdtReducingData<K extends Comparable<K>, S> {
		final K key;
		@Nullable
		final S state;
		final long timestamp;

		CrdtReducingData(K key, @Nullable S state, long timestamp) {
			this.key = key;
			this.state = state;
			this.timestamp = timestamp;
		}
	}

	static class CrdtAccumulator<S> {
		@Nullable
		S state;
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
