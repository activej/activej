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
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.TruncatedDataException;
import io.activej.common.ref.RefBoolean;
import io.activej.common.time.Stopwatch;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.process.frames.ChannelFrameDecoder;
import io.activej.csp.process.frames.ChannelFrameEncoder;
import io.activej.csp.process.frames.FrameFormat;
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
import io.activej.fs.exception.IllegalOffsetException;
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
import java.util.function.BiFunction;
import java.util.function.Function;

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
	private final FrameFormat frameFormat;

	private final StreamRegistry<String> streamReads = StreamRegistry.create();
	private final StreamRegistry<String> streamWrites = StreamRegistry.create();

	private final StreamStatsDetailed<ByteBuf> streamReadStats = StreamStats.detailed(forByteBufs());
	private final StreamStatsDetailed<ByteBuf> streamWriteStats = StreamStats.detailed(forByteBufs());

	private MultilogImpl(Eventloop eventloop, ActiveFs fs, FrameFormat frameFormat, BinarySerializer<T> serializer,
			LogNamingScheme namingScheme) {
		this.eventloop = eventloop;
		this.fs = fs;
		this.frameFormat = frameFormat;
		this.serializer = serializer;
		this.namingScheme = namingScheme;
	}

	public static <T> MultilogImpl<T> create(Eventloop eventloop, ActiveFs fs, FrameFormat frameFormat, BinarySerializer<T> serializer,
			LogNamingScheme namingScheme) {
		return new MultilogImpl<>(eventloop, fs, frameFormat, serializer, namingScheme);
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

		return Promise.of(StreamConsumer.<T>ofSupplier(
				supplier -> supplier
						.transformWith(ChannelSerializer.create(serializer)
								.withAutoFlushInterval(autoFlushInterval)
								.withInitialBufferSize(bufferSize)
								.withSkipSerializationErrors())
						.transformWith(streamWrites.register(logPartition))
						.transformWith(streamWriteStats)
						.bindTo(new LogStreamChunker(eventloop, fs, namingScheme, logPartition,
								consumer -> consumer.transformWith(
										ChannelFrameEncoder.create(frameFormat)
												.withEncoderResets()))))
				.withAcknowledgement(ack -> ack
						.thenEx(wrapException(e -> new MultilogException("Failed to write logs to partition '" + logPartition + '\'', e)))));
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
				.map(logFiles -> {
					RefBoolean lastFileRef = new RefBoolean(true);
					return readLogFiles(logPartition, startPosition, logFiles.stream()
							.filter(logFile -> {
								if (logFile.compareTo(startPosition.getLogFile()) < 0)
									return false;

								if (endLogFile == null || logFile.compareTo(endLogFile) <= 0) {
									return true;
								}

								lastFileRef.set(false);
								return false;
							})
							.map(logFile -> logFile.equals(startPosition.getLogFile()) ?
									startPosition : LogPosition.create(logFile, 0)
							)
							.sorted()
							.collect(toList()), lastFileRef.get());
				})
				.thenEx(wrapException(e -> new MultilogException("Failed to read logs from partition '" + logPartition + '\'', e)));
	}

	private StreamSupplierWithResult<T, LogPosition> readLogFiles(@NotNull String logPartition, @NotNull LogPosition startPosition, @NotNull List<LogPosition> logFiles, boolean lastFile) {
		SettablePromise<LogPosition> positionPromise = new SettablePromise<>();

		Iterator<StreamSupplier<T>> logFileStreams = new Iterator<StreamSupplier<T>>() {
			final Stopwatch sw = Stopwatch.createUnstarted();

			final Iterator<LogPosition> it = logFiles.iterator();
			final CountingFrameFormat countingFormat = new CountingFrameFormat(frameFormat);
			LogPosition currentPosition;

			@Override
			public boolean hasNext() {
				if (it.hasNext()) return true;
				positionPromise.trySet(getLogPosition());
				return false;
			}

			LogPosition getLogPosition() {
				if (currentPosition == null)
					return startPosition;

				return LogPosition.create(currentPosition.getLogFile(), currentPosition.getPosition() + countingFormat.getCount());
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
														sw, countingFormat.getCount(), e);
											}
											return Promise.of(ChannelSupplier.<ByteBuf>of());
										}
									}
									return Promise.of(fileStream, e);
								})
								.map(fileStream -> {
									countingFormat.resetCount();
									sw.reset().start();
									return fileStream
											.transformWith(streamReads.register(logPartition + ":" + currentLogFile + "@" + position))
											.transformWith(streamReadStats)
											.transformWith(
													ChannelFrameDecoder.create(countingFormat)
															.withDecoderResets())
											.withEndOfStream(eos ->
													eos.thenEx(($, e) -> {
														if (e == null ||
																e instanceof TruncatedDataException && !it.hasNext() && lastFile) {
															return Promise.complete();
														}

														if (ignoreMalformedLogs && e instanceof MalformedDataException) {
															if (logger.isWarnEnabled()) {
																logger.warn("Ignoring malformed log file {}:`{}` in {}, previous position: {}",
																		fs, namingScheme.path(logPartition, currentPosition.getLogFile()),
																		sw, countingFormat.getCount(), e);
															}
															return Promise.complete();
														}
														return Promise.ofException(e);
													}))
											.transformWith(ChannelDeserializer.create(serializer))
											.withEndOfStream(eos ->
													eos.whenComplete(($, e) -> log(e)));
								}));
			}

			private void log(Throwable e) {
				if (e == null && logger.isTraceEnabled()) {
					logger.trace("Finish log file {}:`{}` in {}, compressed bytes: {} ({} bytes/s)",
							fs, namingScheme.path(logPartition, currentPosition.getLogFile()),
							sw, countingFormat.getCount(), countingFormat.getCount() / Math.max(sw.elapsed(SECONDS), 1));
				} else if (e != null && logger.isErrorEnabled()) {
					logger.error("Error on log file {}:`{}` in {}, compressed bytes: {} ({} bytes/s)",
							fs, namingScheme.path(logPartition, currentPosition.getLogFile()),
							sw, countingFormat.getCount(), countingFormat.getCount() / Math.max(sw.elapsed(SECONDS), 1), e);
				}
			}
		};

		return StreamSupplierWithResult.of(StreamSupplier.concat(logFileStreams), positionPromise);
	}

	private static void validateLogPartition(@NotNull String logPartition) {
		checkArgument(!logPartition.contains("-"), "Using dash (-) in log partition name is not allowed");
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}


	private static <T> BiFunction<T, @Nullable Throwable, Promise<? extends T>> wrapException(Function<Throwable, Throwable> wrapFn) {
		return (v, e) -> e == null ?
				Promise.of(v) :
				Promise.ofException(wrapFn.apply(e));
	}
}
