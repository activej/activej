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
import io.activej.common.ApplicationSettings;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.exception.TruncatedDataException;
import io.activej.crdt.CrdtData;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.primitives.CrdtType;
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.crdt.util.CrdtDataBinarySerializer;
import io.activej.csp.file.ChannelFileReader;
import io.activej.csp.process.frame.ChannelFrameDecoder;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.processor.reducer.Reducer;
import io.activej.datastream.processor.reducer.StreamReducer;
import io.activej.datastream.processor.transformer.sort.StreamSorter;
import io.activej.datastream.processor.transformer.sort.StreamSorterStorage;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.stats.LongValueStats;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static io.activej.async.function.AsyncRunnables.coalesce;
import static io.activej.common.function.FunctionEx.identity;
import static io.activej.crdt.util.Utils.deleteWalFiles;
import static io.activej.crdt.util.Utils.getWalFiles;
import static io.activej.crdt.wal.FileWriteAheadLog.FRAME_FORMAT;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.util.Comparator.naturalOrder;
import static java.util.stream.Collectors.toList;

public final class WalUploader<K extends Comparable<K>, S> extends AbstractReactive
	implements ReactiveJmxBeanWithStats {

	private static final Logger logger = LoggerFactory.getLogger(WalUploader.class);

	private static final int DEFAULT_SORT_ITEMS_IN_MEMORY = ApplicationSettings.getInt(WalUploader.class, "sortItemsInMemory", 100_000);
	private static final Duration SMOOTHING_WINDOW = ApplicationSettings.getDuration(WalUploader.class, "smoothingWindow", Duration.ofMinutes(5));

	private final AsyncRunnable uploadToStorage = coalesce(this::doUploadToStorage);

	private final Executor executor;
	private final Path path;
	private final CrdtFunction<S> function;
	private final CrdtDataBinarySerializer<K, S> serializer;
	private final ICrdtStorage<K, S> storage;

	private final PromiseStats uploadPromise = PromiseStats.create(SMOOTHING_WINDOW);
	private final LongValueStats totalFilesUploaded = LongValueStats.create(SMOOTHING_WINDOW);
	private final LongValueStats totalFilesUploadedSize = LongValueStats.builder(SMOOTHING_WINDOW)
		.withUnit("bytes")
		.build();
	private boolean detailedMonitoring;

	private @Nullable Path sortDir;
	private int sortItemsInMemory = DEFAULT_SORT_ITEMS_IN_MEMORY;

	private WalUploader(Reactor reactor, Executor executor, Path path, CrdtFunction<S> function, CrdtDataBinarySerializer<K, S> serializer, ICrdtStorage<K, S> storage) {
		super(reactor);
		this.executor = executor;
		this.path = path;
		this.function = function;
		this.serializer = serializer;
		this.storage = storage;
	}

	public static <K extends Comparable<K>, S> WalUploader<K, S> create(Reactor reactor, Executor executor, Path path, CrdtFunction<S> function, CrdtDataBinarySerializer<K, S> serializer, ICrdtStorage<K, S> storage) {
		return builder(reactor, executor, path, function, serializer, storage).build();
	}

	public static <K extends Comparable<K>, S extends CrdtType<S>> WalUploader<K, S> create(Reactor reactor, Executor executor, Path path, CrdtDataBinarySerializer<K, S> serializer, ICrdtStorage<K, S> storage) {
		return builder(reactor, executor, path, serializer, storage).build();
	}

	public static <K extends Comparable<K>, S> WalUploader<K, S>.Builder builder(Reactor reactor, Executor executor, Path path, CrdtFunction<S> function, CrdtDataBinarySerializer<K, S> serializer, ICrdtStorage<K, S> storage) {
		return new WalUploader<>(reactor, executor, path, function, serializer, storage).new Builder();
	}

	public static <K extends Comparable<K>, S extends CrdtType<S>> WalUploader<K, S>.Builder builder(Reactor reactor, Executor executor, Path path, CrdtDataBinarySerializer<K, S> serializer, ICrdtStorage<K, S> storage) {
		return new WalUploader<>(reactor, executor, path, CrdtFunction.ofCrdtType(), serializer, storage).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, WalUploader<K, S>> {
		private Builder() {}

		public Builder withSortDir(Path sortDir) {
			checkNotBuilt(this);
			WalUploader.this.sortDir = sortDir;
			return this;
		}

		public Builder withSortItemsInMemory(int sortItemsInMemory) {
			checkNotBuilt(this);
			WalUploader.this.sortItemsInMemory = sortItemsInMemory;
			return this;
		}

		@Override
		protected WalUploader<K, S> doBuild() {
			return WalUploader.this;
		}
	}

	public Promise<Void> uploadToStorage() {
		checkInReactorThread(this);
		return uploadToStorage.run()
			.whenComplete(uploadPromise.recordStats());
	}

	private Promise<Void> doUploadToStorage() {
		return getWalFiles(executor, path)
			.then(this::uploadWaLFiles);
	}

	private Promise<Void> uploadWaLFiles(List<Path> walFiles) throws IOException {
		if (walFiles.isEmpty()) {
			logger.info("Nothing to upload");
			return Promise.complete();
		}

		if (logger.isInfoEnabled()) {
			logger.info("Uploading write ahead logs {}", walFiles.stream().map(Path::getFileName).collect(toList()));
		}

		Path sortDir;
		try {
			sortDir = createSortDir();
		} catch (IOException e) {
			logger.warn("Failed to create temporary sort directory", e);
			throw e;
		}

		return createReducer(walFiles)
			.getOutput()
			.transformWith(createSorter(sortDir))
			.streamTo(storage.upload())
			.whenComplete(() -> cleanup(sortDir))
			.whenResult(() -> {
				totalFilesUploaded.recordValue(walFiles.size());
				if (detailedMonitoring) {
					for (Path walFile : walFiles) {
						try {
							totalFilesUploadedSize.recordValue(Files.size(walFile));
						} catch (IOException e) {
							logger.warn("Could not get the size of uploaded file {}", walFile);
						}
					}
				}
			})
			.then(() -> deleteWalFiles(executor, walFiles));
	}

	private Path createSortDir() throws IOException {
		if (this.sortDir != null) {
			return this.sortDir;
		} else {
			return Files.createTempDirectory("crdt-wal-sort-dir");
		}
	}

	private StreamSorter<K, CrdtData<K, S>> createSorter(Path sortDir) {
		StreamSorterStorage<CrdtData<K, S>> sorterStorage = StreamSorterStorage.create(reactor, executor, serializer, FRAME_FORMAT, sortDir);
		Function<CrdtData<K, S>, K> keyFn = CrdtData::getKey;
		return StreamSorter.create(sorterStorage, keyFn, naturalOrder(), false, sortItemsInMemory);
	}

	private void cleanup(Path sortDir) {
		if (this.sortDir == null) {
			try {
				Files.deleteIfExists(sortDir);
			} catch (IOException ignored) {
			}
		}
	}

	private StreamReducer<K, CrdtData<K, S>, CrdtData<K, S>> createReducer(List<Path> files) {
		StreamReducer<K, CrdtData<K, S>, CrdtData<K, S>> reducer = StreamReducer.create();

		for (Path file : files) {
			ChannelSuppliers.ofPromise(ChannelFileReader.open(executor, file))
				.transformWith(ChannelFrameDecoder.create(FRAME_FORMAT))
				.withEndOfStream(eos -> eos
					.map(identity(),
						e -> {
							if (e instanceof TruncatedDataException) {
								logger.warn("Write ahead log {} was truncated", file);
								return null;
							}
							throw e;
						}))
				.transformWith(ChannelDeserializer.create(serializer))
				.streamTo(reducer.newInput(CrdtData::getKey, new WalReducer()));
		}

		return reducer;
	}

	// region JMX
	@JmxAttribute
	public PromiseStats getUploadPromise() {
		return uploadPromise;
	}

	@JmxAttribute
	public LongValueStats getTotalFilesUploaded() {
		return totalFilesUploaded;
	}

	@JmxAttribute
	public LongValueStats getTotalFilesUploadedSize() {
		return totalFilesUploadedSize;
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

	public final class WalReducer implements Reducer<K, CrdtData<K, S>, CrdtData<K, S>, CrdtData<K, S>> {
		@Override
		public CrdtData<K, S> onFirstItem(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtData<K, S> firstValue) {
			return firstValue;
		}

		@Override
		public CrdtData<K, S> onNextItem(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtData<K, S> nextValue, CrdtData<K, S> accumulator) {
			long timestamp = Math.max(accumulator.getTimestamp(), nextValue.getTimestamp());
			S merged = function.merge(accumulator.getState(), accumulator.getTimestamp(), nextValue.getState(), nextValue.getTimestamp());
			return new CrdtData<>(key, timestamp, merged);
		}

		@Override
		public void onComplete(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtData<K, S> accumulator) {
			stream.accept(accumulator);
		}
	}
}
