package io.activej.crdt.wal;

import io.activej.async.function.AsyncSupplier;
import io.activej.async.function.AsyncSuppliers;
import io.activej.async.service.EventloopService;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.ApplicationSettings;
import io.activej.common.exception.TruncatedDataException;
import io.activej.crdt.CrdtData;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.csp.AbstractChannelConsumer;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.file.ChannelFileReader;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.csp.process.frames.ChannelFrameDecoder;
import io.activej.csp.process.frames.ChannelFrameEncoder;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.datastream.AbstractStreamSupplier;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.processor.StreamReducer;
import io.activej.datastream.processor.StreamReducers;
import io.activej.datastream.processor.StreamSorter;
import io.activej.datastream.processor.StreamSorterStorageImpl;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Utils.nullify;
import static java.util.Comparator.naturalOrder;
import static java.util.stream.Collectors.toSet;

public class FileWriteAheadLog<K extends Comparable<K>, S> implements WriteAheadLog<K, S>, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(FileWriteAheadLog.class);

	public static final String EXT = ".wal";
	public static final FrameFormat FRAME_FORMAT = LZ4FrameFormat.create();

	private static final int DEFAULT_SORT_ITEMS_IN_MEMORY = ApplicationSettings.getInt(FileWriteAheadLog.class, "sortItemsInMemory", 100_000);

	private final Eventloop eventloop;
	private final Executor executor;
	private final Path path;
	private final CrdtFunction<S> function;
	private final CrdtDataSerializer<K, S> serializer;
	private final CrdtStorage<K, S> storage;

	@Nullable
	private Path sortDir;
	private int sortItemsInMemory = DEFAULT_SORT_ITEMS_IN_MEMORY;

	private final Function<CrdtData<K, S>, K> keyFn = CrdtData::getKey;

	private final AsyncSupplier<Void> flush = AsyncSuppliers.coalesce(this::doFlush);

	private WalConsumer consumer;
	private boolean stopping;

	private FileWriteAheadLog(
			Eventloop eventloop,
			Executor executor,
			Path path,
			CrdtFunction<S> function,
			CrdtDataSerializer<K, S> serializer,
			CrdtStorage<K, S> storage
	) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.path = path;
		this.function = function;
		this.serializer = serializer;
		this.storage = storage;
	}

	public static <K extends Comparable<K>, S> FileWriteAheadLog<K, S> create(
			Eventloop eventloop,
			Executor executor,
			Path path,
			CrdtFunction<S> function,
			CrdtDataSerializer<K, S> serializer,
			CrdtStorage<K, S> storage
	) {
		return new FileWriteAheadLog<>(eventloop, executor, path, function, serializer, storage);
	}

	public FileWriteAheadLog<K, S> withSortDir(Path sortDir) {
		this.sortDir = sortDir;
		return this;
	}

	public FileWriteAheadLog<K, S> withSortItemsInMemory(int sortItemsInMemory) {
		this.sortItemsInMemory = sortItemsInMemory;
		return this;
	}

	@Override
	public Promise<Void> put(K key, S value) {
		logger.trace("Putting value {} at key {}", value, key);
		return consumer.accept(new CrdtData<>(key, value));
	}

	@Override
	public Promise<Void> flush() {
		logger.trace("Flush called");
		return flush.get();
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public @NotNull Promise<Void> start() {
		return getWalFiles()
				.then(this::flushWaLFiles)
				.whenResult(() -> this.consumer = createConsumer());
	}

	@Override
	public @NotNull Promise<?> stop() {
		stopping = true;
		return flush();
	}

	private WalConsumer createConsumer() {
		return new WalConsumer(path.resolve(UUID.randomUUID() + EXT));
	}

	private Promise<Void> doFlush() {
		assert this.consumer != null;

		WalConsumer finishedConsumer = this.consumer;
		this.consumer = stopping ? null : createConsumer();

		logger.trace("Begin flushing write ahead log");

		return finishedConsumer.finish()
				.then(this::getWalFiles)
				.whenResult(walFiles -> {
					if (consumer != null) {
						walFiles.remove(consumer.getWalFile());
					}
				})
				.then(this::flushWaLFiles)
				.whenComplete(toLogger(logger, "doFlush", this));
	}

	private Promise<Void> flushWaLFiles(Set<Path> walFiles) {
		if (walFiles.isEmpty()) return Promise.complete();

		logger.info("Flushing write ahead logs {}", walFiles);

		StreamSorter<K, CrdtData<K, S>> sorter;
		Path sortDir;
		try {
			if (this.sortDir != null) {
				sortDir = this.sortDir;
			} else {
				sortDir = Files.createTempDirectory("crdt-wal-sort-dir");
			}
			StreamSorterStorageImpl<CrdtData<K, S>> sorterStorage = StreamSorterStorageImpl.create(executor, serializer, FRAME_FORMAT, sortDir);
			sorter = StreamSorter.create(sorterStorage, keyFn, naturalOrder(), false, sortItemsInMemory);
		} catch (IOException e) {
			return Promise.ofException(e);
		}

		List<Path> walFilesOrdered = new ArrayList<>(walFiles);
		return Promises.toList(walFilesOrdered.stream()
				.map(walFile -> ChannelFileReader.open(executor, walFile)))
				.then(suppliers -> {
					StreamReducer<K, CrdtData<K, S>, CrdtData<K, S>> reducer = StreamReducer.create();

					for (int i = 0, suppliersSize = suppliers.size(); i < suppliersSize; i++) {
						ChannelSupplier<ByteBuf> supplier = suppliers.get(i);
						int finalI = i;
						supplier.transformWith(ChannelFrameDecoder.create(FRAME_FORMAT))
								.withEndOfStream(eos -> eos
										.thenEx(($, e) -> {
											if (e == null) {
												return Promise.complete();
											}
											if (e instanceof TruncatedDataException) {
												logger.warn("Write ahead log {} was truncated", walFilesOrdered.get(finalI));
												return Promise.complete();
											}
											return Promise.ofException(e);
										}))
								.transformWith(ChannelDeserializer.create(serializer))
								.streamTo(reducer.newInput(CrdtData::getKey, new WalReducer()));
					}

					return reducer.getOutput()
							.transformWith(sorter)
							.streamTo(storage.upload())
							.whenComplete(() -> {
								if (this.sortDir == null) {
									try {
										Files.deleteIfExists(sortDir);
									} catch (IOException ignored) {
									}
								}
							});
				})
				.then(() -> deleteWalFiles(walFiles));
	}

	private Promise<Set<Path>> getWalFiles() {
		return Promise.ofBlockingCallable(executor, () -> Files.list(path)
				.filter(file -> Files.isRegularFile(file) &&
						file.toString().endsWith(EXT))
				.collect(toSet()))
				.whenResult(walFiles -> logger.trace("Found {} write ahead logs {}", walFiles.size(), walFiles));
	}

	private Promise<Void> deleteWalFiles(Set<Path> walFiles) {
		logger.trace("Deleting write ahead logs: {}", walFiles);
		return Promise.ofBlockingRunnable(executor, () -> {
			for (Path walFile : walFiles) {
				Files.deleteIfExists(walFile);
			}
		});
	}

	private final class WalReducer implements StreamReducers.Reducer<K, CrdtData<K, S>, CrdtData<K, S>, CrdtData<K, S>> {

		@Override
		public CrdtData<K, S> onFirstItem(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtData<K, S> firstValue) {
			return firstValue;
		}

		@Override
		public CrdtData<K, S> onNextItem(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtData<K, S> nextValue, CrdtData<K, S> accumulator) {
			return new CrdtData<>(key, function.merge(accumulator.getState(), nextValue.getState()));
		}

		@Override
		public void onComplete(StreamDataAcceptor<CrdtData<K, S>> stream, K key, CrdtData<K, S> accumulator) {
			stream.accept(accumulator);
		}
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
					.streamTo(new AbstractChannelConsumer<ByteBuf>(writer) {
						@Override
						protected Promise<Void> doAccept(@Nullable ByteBuf value) {
							return writer.accept(value)
									.whenComplete((result, e) -> writeCallback = nullify(writeCallback, cb -> cb.accept(result, e)));
						}
					})));
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
}
