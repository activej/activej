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

package io.activej.multilog;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.MemSize;
import io.activej.common.exception.parse.TruncatedDataException;
import io.activej.common.time.Stopwatch;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.process.ChannelLZ4Compressor;
import io.activej.csp.process.ChannelLZ4Decompressor;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.StreamSupplierWithResult;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.stats.StreamRegistry;
import io.activej.datastream.stats.StreamStats;
import io.activej.datastream.stats.StreamStatsDetailed;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.fs.ActiveFs;
import io.activej.fs.exception.scalar.IllegalOffsetException;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static io.activej.common.Checks.checkArgument;
import static io.activej.datastream.stats.StreamStatsSizeCounter.forByteBufs;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public final class MultilogImpl<T> implements Multilog<T>, EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(MultilogImpl.class);

	public static final MemSize DEFAULT_BUFFER_SIZE = MemSize.kilobytes(256);

	private final Eventloop eventloop;
	private final ActiveFs fs;
	private final LogNamingScheme namingScheme;
	private final BinarySerializer<T> serializer;

	private MemSize bufferSize = DEFAULT_BUFFER_SIZE;
	private Duration autoFlushInterval = null;
	private boolean ignoreMalformedLogs;

	private final StreamRegistry<String> streamReads = StreamRegistry.create();
	private final StreamRegistry<String> streamWrites = StreamRegistry.create();

	private final StreamStatsDetailed<ByteBuf> streamReadStats = StreamStats.detailed(forByteBufs());
	private final StreamStatsDetailed<ByteBuf> streamWriteStats = StreamStats.detailed(forByteBufs());

	private MultilogImpl(Eventloop eventloop, ActiveFs fs, BinarySerializer<T> serializer,
			LogNamingScheme namingScheme) {
		this.eventloop = eventloop;
		this.fs = fs;
		this.serializer = serializer;
		this.namingScheme = namingScheme;
	}

	public static <T> MultilogImpl<T> create(Eventloop eventloop, ActiveFs fs,
			BinarySerializer<T> serializer, LogNamingScheme namingScheme) {
		return new MultilogImpl<>(eventloop, fs, serializer, namingScheme);
	}

	public MultilogImpl<T> withBufferSize(int bufferSize) {
		this.bufferSize = MemSize.of(bufferSize);
		return this;
	}

	public MultilogImpl<T> withBufferSize(MemSize bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public MultilogImpl<T> withAutoFlushInterval(Duration autoFlushInterval) {
		this.autoFlushInterval = autoFlushInterval;
		return this;
	}

	public MultilogImpl<T> withIgnoreMalformedLogs(boolean ignore) {
		this.ignoreMalformedLogs = ignore;
		return this;
	}

	@Override
	public Promise<StreamConsumer<T>> write(@NotNull String logPartition) {
		validateLogPartition(logPartition);

		return Promise.of(StreamConsumer.ofSupplier(
				supplier -> supplier
						.transformWith(ChannelSerializer.create(serializer)
								.withAutoFlushInterval(autoFlushInterval)
								.withInitialBufferSize(bufferSize)
								.withSkipSerializationErrors())
						.transformWith(ChannelLZ4Compressor.createFastCompressor())
						.transformWith(streamWrites.register(logPartition))
						.transformWith(streamWriteStats)
						.bindTo(new LogStreamChunker(eventloop, fs, namingScheme, logPartition))));
	}

	@Override
	public Promise<StreamSupplierWithResult<T, LogPosition>> read(@NotNull String logPartition,
			@NotNull LogFile startLogFile, long startOffset,
			@Nullable LogFile endLogFile) {
		validateLogPartition(logPartition);
		LogPosition startPosition = LogPosition.create(startLogFile, startOffset);
		return fs.list(namingScheme.getListGlob(logPartition))
				.map(files ->
						files.keySet().stream()
								.map(namingScheme::parse)
								.filter(Objects::nonNull)
								.filter(partitionAndFile -> partitionAndFile.getLogPartition().equals(logPartition))
								.map(PartitionAndFile::getLogFile)
								.collect(toList()))
				.map(logFiles ->
						logFiles.stream()
								.filter(logFile -> isFileInRange(logFile, startPosition, endLogFile))
								.map(logFile -> logFile.equals(startPosition.getLogFile()) ?
										startPosition : LogPosition.create(logFile, 0)
								)
								.sorted()
								.collect(toList()))
				.map(logFilesToRead -> readLogFiles(logPartition, startPosition, logFilesToRead));
	}

	private StreamSupplierWithResult<T, LogPosition> readLogFiles(@NotNull String logPartition, @NotNull LogPosition startPosition, @NotNull List<LogPosition> logFiles) {
		SettablePromise<LogPosition> positionPromise = new SettablePromise<>();

		Iterator<StreamSupplier<T>> logFileStreams = new Iterator<StreamSupplier<T>>() {
			final Stopwatch sw = Stopwatch.createUnstarted();

			final Iterator<LogPosition> it = logFiles.iterator();
			LogPosition currentPosition;
			long inputStreamPosition;

			@Override
			public boolean hasNext() {
				if (it.hasNext()) return true;
				positionPromise.trySet(getLogPosition());
				return false;
			}

			LogPosition getLogPosition() {
				if (currentPosition == null)
					return startPosition;

				return LogPosition.create(currentPosition.getLogFile(), currentPosition.getPosition() + inputStreamPosition);
			}

			@Override
			public StreamSupplier<T> next() {
				currentPosition = it.next();
				long position = currentPosition.getPosition();
				LogFile currentLogFile = currentPosition.getLogFile();
				if (logger.isTraceEnabled())
					logger.trace("Read log file `{}` from: {}", currentLogFile, position);

				return StreamSupplier.ofPromise(
						fs.download(namingScheme.path(logPartition, currentLogFile), position, Long.MAX_VALUE)
								.thenEx((fileStream, e) -> {
									if (e != null) {
										if (ignoreMalformedLogs && e instanceof IllegalOffsetException) {
											if (logger.isWarnEnabled()) {
												logger.warn("Ignoring log file whose size is less than log position {} {}:`{}` in {}, previous position: {}",
														position, fs, namingScheme.path(logPartition, currentPosition.getLogFile()),
														sw, inputStreamPosition, e);
											}
											return Promise.of(StreamSupplier.closing());
										}
										return Promise.ofException(e);
									}

									inputStreamPosition = 0L;
									sw.reset().start();
									return Promise.of(fileStream
											.transformWith(streamReads.register(logPartition + ":" + currentLogFile + "@" + position))
											.transformWith(streamReadStats)
											.transformWith(ChannelLZ4Decompressor.create()
													.withInspector(new ChannelLZ4Decompressor.Inspector() {
														@Override
														public <Q extends ChannelLZ4Decompressor.Inspector> Q lookup(Class<Q> type) {
															throw new UnsupportedOperationException();
														}

														@Override
														public void onBlock(ChannelLZ4Decompressor self, ChannelLZ4Decompressor.Header header, ByteBuf inputBuf, ByteBuf outputBuf) {
															inputStreamPosition += ChannelLZ4Decompressor.HEADER_LENGTH + header.compressedLen;
														}
													}))
											.transformWith(supplier ->
													supplier.withEndOfStream(eos ->
															eos.thenEx(($, e2) -> {
																if (e2 == null) {
																	return Promise.complete();
																}
																if (ignoreMalformedLogs &&
																		e2 instanceof TruncatedDataException ||
																		e2 == ChannelLZ4Decompressor.STREAM_IS_CORRUPTED ||
																		e2 == BinaryChannelSupplier.UNEXPECTED_DATA_EXCEPTION) {
																	if (logger.isWarnEnabled()) {
																		logger.warn("Ignoring malformed log file {}:`{}` in {}, previous position: {}",
																				fs, namingScheme.path(logPartition, currentPosition.getLogFile()),
																				sw, inputStreamPosition, e2);
																	}
																	return Promise.complete();
																}
																return Promise.ofException(e2);
															})))
											.transformWith(ChannelDeserializer.create(serializer))
											.withEndOfStream(eos ->
													eos.whenComplete(($, e2) -> log(e2))));
								}));
			}

			private void log(Throwable e) {
				if (e == null && logger.isTraceEnabled()) {
					logger.trace("Finish log file {}:`{}` in {}, compressed bytes: {} ({} bytes/s)",
							fs, namingScheme.path(logPartition, currentPosition.getLogFile()),
							sw, inputStreamPosition, inputStreamPosition / Math.max(sw.elapsed(SECONDS), 1));
				} else if (e != null && logger.isErrorEnabled()) {
					logger.error("Error on log file {}:`{}` in {}, compressed bytes: {} ({} bytes/s)",
							fs, namingScheme.path(logPartition, currentPosition.getLogFile()),
							sw, inputStreamPosition, inputStreamPosition / Math.max(sw.elapsed(SECONDS), 1), e);
				}
			}
		};

		return StreamSupplierWithResult.of(StreamSupplier.concat(logFileStreams), positionPromise);
	}

	private static void validateLogPartition(@NotNull String logPartition) {
		checkArgument(!logPartition.contains("-"), "Using dash (-) in log partition name is not allowed");
	}

	private static boolean isFileInRange(@NotNull LogFile logFile, @NotNull LogPosition startPosition, @Nullable LogFile endFile) {
		if (logFile.compareTo(startPosition.getLogFile()) < 0)
			return false;

		return endFile == null || logFile.compareTo(endFile) <= 0;
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

}
