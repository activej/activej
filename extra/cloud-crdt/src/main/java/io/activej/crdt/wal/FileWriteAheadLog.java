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

package io.activej.crdt.wal;

import io.activej.async.function.AsyncRunnable;
import io.activej.async.function.AsyncRunnables;
import io.activej.async.service.EventloopService;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.ApplicationSettings;
import io.activej.common.initializer.WithInitializer;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.crdt.CrdtData;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.csp.process.frames.ChannelFrameEncoder;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.datastream.AbstractStreamSupplier;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanWithStats;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.stats.EventStats;
import io.activej.jmx.stats.ValueStats;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.activej.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Checks.checkArgument;
import static io.activej.crdt.util.Utils.deleteWalFiles;
import static io.activej.crdt.util.Utils.getWalFiles;
import static io.activej.crdt.wal.FileWriteAheadLog.FlushMode.*;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;

public class FileWriteAheadLog<K extends Comparable<K>, S> implements WriteAheadLog<K, S>, EventloopService,
		EventloopJmxBeanWithStats, WithInitializer<FileWriteAheadLog<K, S>> {
	private static final Logger logger = LoggerFactory.getLogger(FileWriteAheadLog.class);

	public static final String EXT_FINAL = ".wal";
	public static final String EXT_CURRENT = ".current";
	public static final FrameFormat FRAME_FORMAT = LZ4FrameFormat.create();

	private static final Duration SMOOTHING_WINDOW = ApplicationSettings.getDuration(FileWriteAheadLog.class, "smoothingWindow", Duration.ofMinutes(5));

	private final Eventloop eventloop;
	private final Executor executor;
	private final Path path;
	private final CrdtDataSerializer<K, S> serializer;
	private final FlushMode flushMode;

	private final WalUploader<K, S> uploader;

	private final AsyncRunnable flush = AsyncRunnables.coalesce(this::doFlush);

	private WalConsumer consumer;
	private boolean stopping;
	private boolean flushRequired;
	private boolean scanLostFiles = true;

	private CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	// region JMX
	private final PromiseStats putPromise = PromiseStats.create(SMOOTHING_WINDOW);
	private final PromiseStats flushPromise = PromiseStats.create(SMOOTHING_WINDOW);
	private final EventStats totalPuts = EventStats.create(SMOOTHING_WINDOW);
	private final EventStats totalFlushes = EventStats.create(SMOOTHING_WINDOW);
	private final ValueStats totalFlushedSize = ValueStats.create(SMOOTHING_WINDOW).withUnit("bytes");
	private boolean detailedMonitoring;
	// endregion

	private FileWriteAheadLog(
			Eventloop eventloop,
			Executor executor,
			Path path,
			CrdtDataSerializer<K, S> serializer,
			FlushMode flushMode,
			@Nullable WalUploader<K, S> uploader
	) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.path = path;
		this.serializer = serializer;
		this.flushMode = flushMode;
		this.uploader = uploader;
	}

	public static <K extends Comparable<K>, S> FileWriteAheadLog<K, S> create(
			Eventloop eventloop,
			Executor executor,
			Path path,
			CrdtDataSerializer<K, S> serializer,
			WalUploader<K, S> uploader
	) {
		return new FileWriteAheadLog<>(eventloop, executor, path, serializer, UPLOAD_TO_STORAGE, uploader);
	}

	public static <K extends Comparable<K>, S> FileWriteAheadLog<K, S> create(
			Eventloop eventloop,
			Executor executor,
			Path path,
			CrdtDataSerializer<K, S> serializer,
			FlushMode flushMode
	) {
		checkArgument(flushMode == ROTATE_FILE || flushMode == ROTATE_FILE_AWAIT);
		return new FileWriteAheadLog<>(eventloop, executor, path, serializer, flushMode, null);
	}

	public FileWriteAheadLog<K, S> withCurrentTimeProvider(CurrentTimeProvider now) {
		this.now = now;
		return this;
	}

	public FlushMode getFlushMode() {
		return flushMode;
	}

	@Override
	public Promise<Void> put(K key, S value) {
		logger.trace("Putting value {} at key {}", value, key);
		totalPuts.recordEvent();

		flushRequired = true;
		return consumer.accept(new CrdtData<>(key, now.currentTimeMillis(), value))
				.whenComplete(putPromise.recordStats());
	}

	@Override
	public Promise<Void> flush() {
		logger.trace("Flush called");
		return flush.run()
				.whenComplete(flushPromise.recordStats());
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public @NotNull Promise<Void> start() {
		return scanLostFiles()
				.then(this::flushFiles)
				.whenResult(() -> this.consumer = createConsumer());
	}

	@Override
	public @NotNull Promise<?> stop() {
		stopping = true;
		if (flushRequired) return flush();

		return deleteWalFiles(executor, singleton(consumer.walFile));
	}

	private @Nullable WalConsumer createConsumer() {
		return stopping ? null : new WalConsumer(path.resolve(UUID.randomUUID() + EXT_CURRENT));
	}

	private Promise<Void> doFlush() {
		if (!flushRequired) {
			logger.trace("Nothing to flush");
			return Promise.complete();
		}
		flushRequired = false;
		totalFlushes.recordEvent();

		logger.trace("Begin flushing write ahead log");

		WalConsumer finishedConsumer = consumer;
		consumer = createConsumer();

		if (detailedMonitoring) {
			try {
				totalFlushedSize.recordValue(Files.size(finishedConsumer.walFile));
			} catch (IOException e) {
				logger.warn("Could not get the size of flushed file {}", finishedConsumer.walFile);
			}
		}

		return finishedConsumer.finish()
				.then(() -> Promise.ofBlocking(executor, () -> rename(finishedConsumer.walFile))
						.whenException(e -> scanLostFiles = true))
				.then(this::scanLostFiles)
				.then(this::flushFiles)
				.whenException(e -> flushRequired = true)
				.whenComplete(toLogger(logger, TRACE, TRACE, "doFlush", this));
	}

	private Promise<Void> flushFiles() {
		if (flushMode == ROTATE_FILE) {
			return Promise.complete();
		} else if (flushMode == ROTATE_FILE_AWAIT) {
			return awaitExternalFlush();
		} else {
			assert flushMode == UPLOAD_TO_STORAGE && uploader != null;

			return uploader.uploadToStorage();
		}
	}

	private Promise<Void> scanLostFiles() {
		if (!scanLostFiles) return Promise.complete();

		return getLostFiles()
				.then(lostFiles ->
						Promise.ofBlocking(executor, () -> {
							for (Path lostFile : lostFiles) {
								rename(lostFile);
							}
						}))
				.whenResult(() -> scanLostFiles = false);
	}

	private void rename(Path from) throws IOException {
		assert from.toString().endsWith(EXT_CURRENT);
		assert consumer == null || !from.equals(consumer.getWalFile());

		String filename = from.getFileName().toString();
		Path to = from.resolveSibling(filename.replace(EXT_CURRENT, EXT_FINAL));
		try {
			Files.move(from, to, ATOMIC_MOVE);
		} catch (AtomicMoveNotSupportedException ignored) {
			Files.move(from, to);
		}
	}

	private Promise<Void> awaitExternalFlush() {
		return getWalFiles(executor, path)
				.thenIfElse(List::isEmpty,
						$ -> Promise.complete(),
						$ -> Promises.delay(Duration.ofSeconds(1))
								.then(this::awaitExternalFlush));
	}

	private Promise<List<Path>> getLostFiles() {
		return Promise.ofBlocking(executor,
						() -> {
							try (Stream<Path> list = Files.list(path)) {
								return list
										.filter(file -> Files.isRegularFile(file) &&
												file.toString().endsWith(EXT_CURRENT) &&
												(consumer == null || !file.equals(consumer.getWalFile())))
										.collect(toList());
							}
						})
				.whenResult(walFiles -> {
					if (logger.isTraceEnabled()) {
						logger.trace("Found {} lost files {}", walFiles.size(), walFiles.stream().map(Path::getFileName).collect(toList()));
					}
				});
	}

	private final class WalConsumer {
		private final AbstractStreamSupplier<CrdtData<K, S>> internalSupplier = new AbstractStreamSupplier<CrdtData<K, S>>() {
			@Override
			protected void onStarted() {
				resume();
			}
		};
		private final Path walFile;

		private SettablePromise<Void> writeCallback;

		public WalConsumer(Path walFile) {
			this.walFile = walFile;
			ChannelConsumer<ByteBuf> writer = ChannelConsumer.ofPromise(ChannelFileWriter.open(executor, walFile));
			internalSupplier.streamTo(StreamConsumer.ofSupplier(supplier -> supplier
					.transformWith(ChannelSerializer.create(serializer)
							.withAutoFlushInterval(Duration.ZERO))
					.transformWith(ChannelFrameEncoder.create(FRAME_FORMAT))
					.streamTo(ChannelConsumer.of(value -> {
						if (this.writeCallback == null) return writer.accept(value);

						SettablePromise<Void> writeCallback = this.writeCallback;
						this.writeCallback = null;
						return writer.accept(value)
								.whenComplete(writeCallback::accept);
					}))));
		}

		public Path getWalFile() {
			return walFile;
		}

		public Promise<Void> accept(CrdtData<K, S> data) {
			if (this.writeCallback == null) {
				this.writeCallback = new SettablePromise<>();
			}
			SettablePromise<Void> writeCallback = this.writeCallback;
			internalSupplier.send(data);
			return writeCallback;
		}

		public Promise<Void> finish() {
			internalSupplier.sendEndOfStream();
			return internalSupplier.getAcknowledgement();
		}
	}

	public enum FlushMode {
		UPLOAD_TO_STORAGE,
		ROTATE_FILE,
		ROTATE_FILE_AWAIT
	}

	// region JMX
	@JmxAttribute
	public PromiseStats getPutPromise() {
		return putPromise;
	}

	@JmxAttribute
	public PromiseStats getFlushPromise() {
		return flushPromise;
	}

	@JmxAttribute
	public EventStats getTotalPuts() {
		return totalPuts;
	}

	@JmxAttribute
	public EventStats getTotalFlushes() {
		return totalFlushes;
	}

	@JmxAttribute
	public ValueStats getTotalFlushedSize() {
		return totalFlushedSize;
	}

	@JmxAttribute
	public boolean isDetailedMonitoring() {
		return detailedMonitoring;
	}

	@JmxOperation
	public void startDetailedMonitoring() {
		detailedMonitoring = true;
	}

	@JmxOperation
	public void stopDetailedMonitoring() {
		detailedMonitoring = false;
	}
	// endregion
}
