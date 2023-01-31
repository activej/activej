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

package io.activej.aggregation;

import io.activej.aggregation.ot.AggregationStructure;
import io.activej.async.function.AsyncSupplier;
import io.activej.async.service.ReactiveService;
import io.activej.bytebuf.ByteBuf;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.MemSize;
import io.activej.common.Utils;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.ref.RefInt;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.process.ChannelByteChunker;
import io.activej.csp.process.frames.ChannelFrameDecoder;
import io.activej.csp.process.frames.ChannelFrameEncoder;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.stats.BasicStreamStats;
import io.activej.datastream.stats.DetailedStreamStats;
import io.activej.datastream.stats.StreamStats;
import io.activej.fs.IFileSystem;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.stats.ExceptionStats;
import io.activej.jmx.stats.StatsUtils;
import io.activej.jmx.stats.ValueStats;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.function.Predicate;

import static io.activej.aggregation.util.Utils.createBinarySerializer;
import static io.activej.async.util.LogUtils.thisMethod;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.difference;
import static io.activej.datastream.stats.StreamStatsSizeCounter.forByteBufs;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.slf4j.LoggerFactory.getLogger;

@SuppressWarnings("rawtypes") // JMX doesn't work with generic types
public final class AggregationChunkStorage<C> extends AbstractReactive
		implements IAggregationChunkStorage<C>, ReactiveService, ReactiveJmxBeanWithStats {
	private static final Logger logger = getLogger(AggregationChunkStorage.class);
	public static final MemSize DEFAULT_BUFFER_SIZE = MemSize.kilobytes(256);

	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);
	public static final String DEFAULT_BACKUP_PATH = "backups";
	public static final String SUCCESSFUL_BACKUP_FILE = "_0_SUCCESSFUL_BACKUP";
	public static final String LOG = ".log";
	public static final String TEMP_LOG = ".temp";

	private final ChunkIdJsonCodec<C> chunkIdCodec;
	private final AsyncSupplier<C> idGenerator;
	private final FrameFormat frameFormat;

	private final IFileSystem fileSystem;
	private String chunksPath = "";
	private String tempPath = "";
	private String backupPath = DEFAULT_BACKUP_PATH;

	private MemSize bufferSize = DEFAULT_BUFFER_SIZE;

	private final ValueStats chunksCount = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseAsyncSupplier = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseOpenR = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseOpenW = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseFinishChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseList = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseBackup = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseCleanup = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseCleanupCheckRequiredChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	private boolean detailed;

	private final DetailedStreamStats<ByteBuf> readFile = StreamStats.<ByteBuf>detailedBuilder()
			.withSizeCounter(forByteBufs())
			.build();
	private final DetailedStreamStats<ByteBuf> readDecompress = StreamStats.<ByteBuf>detailedBuilder()
			.withSizeCounter(forByteBufs())
			.build();
	private final BasicStreamStats<?> readDeserialize = StreamStats.basic();
	private final DetailedStreamStats<?> readDeserializeDetailed = StreamStats.detailed();

	private final BasicStreamStats<?> writeSerialize = StreamStats.basic();
	private final DetailedStreamStats<?> writeSerializeDetailed = StreamStats.detailed();
	private final DetailedStreamStats<ByteBuf> writeCompress = StreamStats.<ByteBuf>detailedBuilder()
			.withSizeCounter(forByteBufs())
			.build();
	private final DetailedStreamStats<ByteBuf> writeChunker = StreamStats.<ByteBuf>detailedBuilder()
			.withSizeCounter(forByteBufs())
			.build();
	private final DetailedStreamStats<ByteBuf> writeFile = StreamStats.<ByteBuf>detailedBuilder()
			.withSizeCounter(forByteBufs())
			.build();

	private final ExceptionStats chunkNameWarnings = ExceptionStats.create();
	private int cleanupPreservedFiles;
	private int cleanupDeletedFiles;
	private int cleanupDeletedFilesTotal;
	private int cleanupSkippedFiles;
	private int cleanupSkippedFilesTotal;

	private int finishChunks;

	private AggregationChunkStorage(Reactor reactor, ChunkIdJsonCodec<C> chunkIdCodec, AsyncSupplier<C> idGenerator, FrameFormat frameFormat, IFileSystem fileSystem) {
		super(reactor);
		this.chunkIdCodec = chunkIdCodec;
		this.idGenerator = idGenerator;
		this.frameFormat = frameFormat;
		this.fileSystem = fileSystem;
	}

	public static <C> AggregationChunkStorage<C> create(Reactor reactor,
			ChunkIdJsonCodec<C> chunkIdCodec,
			AsyncSupplier<C> idGenerator, FrameFormat frameFormat, IFileSystem fileSystem) {
		return builder(reactor, chunkIdCodec, idGenerator, frameFormat, fileSystem).build();
	}

	public static <C> AggregationChunkStorage<C>.Builder builder(Reactor reactor,
			ChunkIdJsonCodec<C> chunkIdCodec,
			AsyncSupplier<C> idGenerator, FrameFormat frameFormat, IFileSystem fileSystem) {
		return new AggregationChunkStorage<>(reactor, chunkIdCodec, idGenerator, frameFormat, fileSystem).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, AggregationChunkStorage<C>>{
		private Builder() {}

		public Builder withBufferSize(MemSize bufferSize) {
			checkNotBuilt(this);
			AggregationChunkStorage.this.bufferSize = bufferSize;
			return this;
		}

		public Builder withChunksPath(String path) {
			checkNotBuilt(this);
			AggregationChunkStorage.this.chunksPath = path;
			return this;
		}

		public Builder withTempPath(String path) {
			checkNotBuilt(this);
			AggregationChunkStorage.this.tempPath = path;
			return this;
		}

		public Builder withBackupPath(String path) {
			checkNotBuilt(this);
			AggregationChunkStorage.this.backupPath = path;
			return this;
		}

		@Override
		protected AggregationChunkStorage<C> doBuild() {
			return AggregationChunkStorage.this;
		}
	}


	@SuppressWarnings("unchecked")
	@Override
	public <T> Promise<StreamSupplier<T>> read(AggregationStructure aggregation, List<String> fields,
			Class<T> recordClass, C chunkId,
			DefiningClassLoader classLoader) {
		checkInReactorThread(this);
		return fileSystem.download(toPath(chunkId))
				.mapException(e -> new AggregationException("Failed to download chunk '" + chunkId + '\'', e))
				.whenComplete(promiseOpenR.recordStats())
				.map(supplier -> supplier
						.transformWith(readFile)
						.transformWith(ChannelFrameDecoder.create(frameFormat))
						.transformWith(readDecompress)
						.transformWith(ChannelDeserializer.create(
								createBinarySerializer(aggregation, recordClass, aggregation.getKeys(), fields, classLoader)))
						.transformWith((StreamStats<T>) (detailed ? readDeserializeDetailed : readDeserialize))
						.withEndOfStream(eos -> eos
								.mapException(e -> new AggregationException("Failed to read chunk '" + chunkId + '\'', e))));
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Promise<StreamConsumer<T>> write(AggregationStructure aggregation, List<String> fields,
			Class<T> recordClass, C chunkId,
			DefiningClassLoader classLoader) {
		checkInReactorThread(this);
		return fileSystem.upload(toTempPath(chunkId))
				.mapException(e -> new AggregationException("Failed to upload chunk '" + chunkId + '\'', e))
				.whenComplete(promiseOpenW.recordStats())
				.map(consumer -> StreamConsumer.<T>ofSupplier(
								supplier -> supplier
										.transformWith((StreamStats<T>) (detailed ? writeSerializeDetailed : writeSerialize))
										.transformWith(ChannelSerializer.builder(
														createBinarySerializer(aggregation, recordClass, aggregation.getKeys(), fields, classLoader))
												.withInitialBufferSize(bufferSize)
												.build())
										.transformWith(writeCompress)
										.transformWith(ChannelFrameEncoder.create(frameFormat))
										.transformWith(writeChunker)
										.transformWith(ChannelByteChunker.create(
												bufferSize.map(bytes -> bytes / 2),
												bufferSize.map(bytes -> bytes * 2)))
										.transformWith(writeFile)
										.streamTo(consumer))
						.withAcknowledgement(ack -> ack.mapException(e -> new AggregationException("Failed to write chunk '" + chunkId + '\'', e))));
	}

	@Override
	public Promise<Void> finish(Set<C> chunkIds) {
		checkInReactorThread(this);
		return fileSystem.moveAll(chunkIds.stream().collect(toMap(this::toTempPath, this::toPath)))
				.mapException(e -> new AggregationException("Failed to finalize chunks: " + Utils.toString(chunkIds), e))
				.whenResult(() -> finishChunks = chunkIds.size())
				.whenComplete(promiseFinishChunks.recordStats());
	}

	@Override
	public Promise<C> createId() {
		checkInReactorThread(this);
		return idGenerator.get()
				.mapException(e -> new AggregationException("Could not create ID", e))
				.whenComplete(promiseAsyncSupplier.recordStats());
	}

	public Promise<Void> backup(String backupId, Set<C> chunkIds) {
		checkInReactorThread(this);
		return fileSystem.copyAll(chunkIds.stream().collect(toMap(this::toPath, c -> toBackupPath(backupId, c))))
				.then(() -> ChannelSupplier.<ByteBuf>of().streamTo(
						fileSystem.upload(toBackupPath(backupId, null), 0)))
				.mapException(e -> new AggregationException("Backup '" + backupId + "' of chunks " + Utils.toString(chunkIds) + " failed", e))
				.whenComplete(promiseBackup.recordStats())
				.toVoid();
	}

	public Promise<Void> cleanup(Set<C> saveChunks) {
		checkInReactorThread(this);
		return cleanup(saveChunks, null);
	}

	public Promise<Void> cleanup(Set<C> preserveChunks, @Nullable Instant instant) {
		checkInReactorThread(this);
		long timestamp = instant != null ? instant.toEpochMilli() : -1;

		RefInt skipped = new RefInt(0);
		RefInt deleted = new RefInt(0);
		return fileSystem.list(toDir(chunksPath) + "*" + LOG)
				.mapException(e -> new AggregationException("Failed to list chunks for cleanup", e))
				.then(list -> {
					Set<String> toDelete = list.entrySet().stream()
							.filter(entry -> {
								C id = fromPath(entry.getKey());
								if (id == null || preserveChunks.contains(id)) {
									return false;
								}
								long fileTimestamp = entry.getValue().getTimestamp();
								if (timestamp == -1 || fileTimestamp <= timestamp) {
									return true;
								}
								logger.trace("File {} timestamp {} > {}", entry, fileTimestamp, timestamp);
								skipped.inc();
								return false;
							})
							.map(entry -> {
								if (logger.isTraceEnabled()) {
									FileTime lastModifiedTime = FileTime.fromMillis(entry.getValue().getTimestamp());
									logger.trace("Delete file: {} with last modifiedTime: {}({} millis)", entry.getKey(),
											lastModifiedTime, lastModifiedTime.toMillis());
								}
								deleted.inc();

								return entry.getKey();
							})
							.collect(toSet());
					if (toDelete.isEmpty()) return Promise.complete();
					return fileSystem.deleteAll(toDelete)
							.mapException(e -> new AggregationException("Failed to clean up chunks", e));
				})
				.whenResult(() -> {
					cleanupPreservedFiles = preserveChunks.size();
					cleanupDeletedFiles = deleted.get();
					cleanupDeletedFilesTotal += deleted.get();
					cleanupSkippedFiles = skipped.get();
					cleanupSkippedFilesTotal += skipped.get();
				})
				.whenComplete(promiseCleanup.recordStats());
	}

	public Promise<Set<C>> list(Predicate<C> chunkIdPredicate, LongPredicate lastModifiedPredicate) {
		checkInReactorThread(this);
		return fileSystem.list(toDir(chunksPath) + "*" + LOG)
				.mapException(e -> new AggregationException("Failed to list chunks", e))
				.map(list ->
						list.entrySet().stream()
								.filter(entry -> lastModifiedPredicate.test(entry.getValue().getTimestamp()))
								.map(Map.Entry::getKey)
								.map(this::fromPath)
								.filter(Objects::nonNull)
								.filter(chunkIdPredicate)
								.collect(toSet()))
				.whenComplete(promiseList.recordStats());
	}

	public Promise<Void> checkRequiredChunks(Set<C> requiredChunks) {
		checkInReactorThread(this);
		return list(s -> true, timestamp -> true)
				.whenResult(actualChunks -> chunksCount.recordValue(actualChunks.size()))
				.then(actualChunks -> actualChunks.containsAll(requiredChunks) ?
						Promise.complete() :
						Promise.ofException(new AggregationException("Missed chunks from storage: " +
								Utils.toString(difference(requiredChunks, actualChunks)))))
				.whenComplete(promiseCleanupCheckRequiredChunks.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), Utils.toString(requiredChunks)));
	}

	private String toPath(C chunkId) {
		return toDir(chunksPath) + chunkIdCodec.toFileName(chunkId) + LOG;
	}

	private String toTempPath(C chunkId) {
		return toDir(tempPath) + chunkIdCodec.toFileName(chunkId) + TEMP_LOG;
	}

	private String toBackupPath(String backupId, @Nullable C chunkId) {
		return toDir(backupPath) + backupId + IFileSystem.SEPARATOR +
				(chunkId != null ? chunkIdCodec.toFileName(chunkId) + LOG : SUCCESSFUL_BACKUP_FILE);
	}

	private String toDir(String path) {
		return path.isEmpty() || path.endsWith(IFileSystem.SEPARATOR) ? path : path + IFileSystem.SEPARATOR;
	}

	private @Nullable C fromPath(String path) {
		String chunksDir = toDir(chunksPath);
		checkArgument(path.startsWith(chunksDir));
		try {
			return chunkIdCodec.fromFileName(path.substring(chunksDir.length(), path.length() - LOG.length()));
		} catch (MalformedDataException e) {
			chunkNameWarnings.recordException(e);
			logger.warn("Invalid chunk filename: {}", path);
			return null;
		}
	}

	@Override
	public Promise<?> start() {
		checkInReactorThread(this);
		return fileSystem.ping()
				.mapException(e -> new AggregationException("Failed to start storage", e));
	}

	@Override
	public Promise<?> stop() {
		checkInReactorThread(this);
		return Promise.complete();
	}

	// region JMX

	@JmxAttribute
	public PromiseStats getPromiseAsyncSupplier() {
		return promiseAsyncSupplier;
	}

	@JmxAttribute
	public PromiseStats getPromiseFinishChunks() {
		return promiseFinishChunks;
	}

	@JmxAttribute
	public PromiseStats getPromiseBackup() {
		return promiseBackup;
	}

	@JmxAttribute
	public PromiseStats getPromiseCleanup() {
		return promiseCleanup;
	}

	@JmxAttribute
	public PromiseStats getPromiseList() {
		return promiseList;
	}

	@JmxAttribute
	public PromiseStats getPromiseOpenR() {
		return promiseOpenR;
	}

	@JmxAttribute
	public PromiseStats getPromiseOpenW() {
		return promiseOpenW;
	}

	@JmxAttribute
	public DetailedStreamStats getReadFile() {
		return readFile;
	}

	@JmxAttribute
	public DetailedStreamStats getReadDecompress() {
		return readDecompress;
	}

	@JmxAttribute
	public BasicStreamStats getReadDeserialize() {
		return readDeserialize;
	}

	@JmxAttribute
	public DetailedStreamStats getReadDeserializeDetailed() {
		return readDeserializeDetailed;
	}

	@JmxAttribute
	public BasicStreamStats getWriteSerialize() {
		return writeSerialize;
	}

	@JmxAttribute
	public DetailedStreamStats getWriteSerializeDetailed() {
		return writeSerializeDetailed;
	}

	@JmxAttribute
	public DetailedStreamStats getWriteCompress() {
		return writeCompress;
	}

	@JmxAttribute
	public DetailedStreamStats getWriteChunker() {
		return writeChunker;
	}

	@JmxAttribute
	public DetailedStreamStats getWriteFile() {
		return writeFile;
	}

	@JmxAttribute
	public int getFinishChunks() {
		return finishChunks;
	}

	@JmxAttribute
	public ExceptionStats getChunkNameWarnings() {
		return chunkNameWarnings;
	}

	@JmxAttribute
	public int getCleanupPreservedFiles() {
		return cleanupPreservedFiles;
	}

	@JmxAttribute
	public int getCleanupDeletedFiles() {
		return cleanupDeletedFiles;
	}

	@JmxAttribute
	public int getCleanupDeletedFilesTotal() {
		return cleanupDeletedFilesTotal;
	}

	@JmxAttribute
	public int getCleanupSkippedFiles() {
		return cleanupSkippedFiles;
	}

	@JmxAttribute
	public int getCleanupSkippedFilesTotal() {
		return cleanupSkippedFilesTotal;
	}

	@JmxAttribute
	public ValueStats getChunksCount() {
		return chunksCount;
	}

	@JmxAttribute
	public PromiseStats getPromiseCleanupCheckRequiredChunks() {
		return promiseCleanupCheckRequiredChunks;
	}

	@JmxOperation
	public void startDetailedMonitoring() {
		detailed = true;
	}

	@JmxOperation
	public void stopDetailedMonitoring() {
		detailed = false;
	}

	@Override
	public void resetStats() {
		cleanupPreservedFiles = 0;
		cleanupDeletedFiles = 0;
		cleanupDeletedFilesTotal = 0;
		cleanupSkippedFiles = 0;
		cleanupSkippedFilesTotal = 0;
		StatsUtils.resetStats(this);
	}
	// endregion
}
