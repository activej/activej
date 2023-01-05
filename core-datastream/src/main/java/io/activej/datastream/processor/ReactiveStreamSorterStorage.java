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

package io.activej.datastream.processor;

import io.activej.common.MemSize;
import io.activej.common.initializer.WithInitializer;
import io.activej.csp.file.ChannelFileReader;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.csp.process.ChannelByteChunker;
import io.activej.csp.process.frames.ChannelFrameDecoder;
import io.activej.csp.process.frames.ChannelFrameEncoder;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.promise.Promise;
import io.activej.serializer.BinarySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static io.activej.common.Checks.checkArgument;
import static java.lang.String.format;

/**
 * This class uses for  splitting a single input stream into smaller partitions during merge sort,
 * for avoid overflow RAM, it write it to  external memory . You can write here data with index
 * of partition and then read it from here and merge.
 *
 * @param <T> type of storing data
 */
public final class ReactiveStreamSorterStorage<T> implements AsyncStreamSorterStorage<T>, WithInitializer<ReactiveStreamSorterStorage<T>> {
	private static final Logger logger = LoggerFactory.getLogger(ReactiveStreamSorterStorage.class);

	public static final String DEFAULT_FILE_PATTERN = "%d";
	public static final MemSize DEFAULT_SORTER_BLOCK_SIZE = MemSize.kilobytes(256);

	private static final AtomicInteger PARTITION = new AtomicInteger();

	private final Executor executor;
	private final BinarySerializer<T> serializer;
	private final FrameFormat frameFormat;
	private final Path path;

	private String filePattern = DEFAULT_FILE_PATTERN;
	private MemSize readBlockSize = ChannelSerializer.DEFAULT_INITIAL_BUFFER_SIZE;
	private MemSize writeBlockSize = DEFAULT_SORTER_BLOCK_SIZE;

	// region creators
	private ReactiveStreamSorterStorage(Executor executor, BinarySerializer<T> serializer,
			FrameFormat frameFormat, Path path) {
		this.executor = executor;
		this.serializer = serializer;
		this.frameFormat = frameFormat;
		this.path = path;
	}

	/**
	 * Creates a new storage
	 *
	 * @param executor   executor for running tasks in new thread
	 * @param serializer for serialization to bytes
	 * @param path       path in which will store received data
	 */
	public static <T> ReactiveStreamSorterStorage<T> create(Executor executor,
			BinarySerializer<T> serializer, FrameFormat frameFormat, Path path) {
		checkArgument(!path.getFileName().toString().contains("%d"), "Filename should not contain '%d'");
		try {
			Files.createDirectories(path);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		return new ReactiveStreamSorterStorage<>(executor, serializer, frameFormat, path);
	}

	public ReactiveStreamSorterStorage<T> withFilePattern(String filePattern) {
		checkArgument(!filePattern.contains("%d"), "File pattern should not contain '%d'");
		this.filePattern = filePattern;
		return this;
	}

	public ReactiveStreamSorterStorage<T> withReadBlockSize(MemSize readBlockSize) {
		this.readBlockSize = readBlockSize;
		return this;
	}

	public ReactiveStreamSorterStorage<T> withWriteBlockSize(MemSize writeBlockSize) {
		this.writeBlockSize = writeBlockSize;
		return this;
	}
	// endregion

	public Path getPath() {
		return path;
	}

	private Path partitionPath(int i) {
		return path.resolve(format(filePattern, i));
	}

	@Override
	public Promise<Integer> newPartitionId() {
		return Promise.of(PARTITION.incrementAndGet());
	}

	@Override
	public Promise<StreamConsumer<T>> write(int partition) {
		Path path = partitionPath(partition);
		return Promise.of(StreamConsumer.ofSupplier(
				supplier -> supplier
						.transformWith(ChannelSerializer.create(serializer)
								.withInitialBufferSize(readBlockSize))
						.transformWith(ChannelByteChunker.create(writeBlockSize.map(bytes -> bytes / 2), writeBlockSize))
						.transformWith(ChannelFrameEncoder.create(frameFormat))
						.transformWith(ChannelByteChunker.create(writeBlockSize.map(bytes -> bytes / 2), writeBlockSize))
						.streamTo(ChannelFileWriter.open(executor, path))));
	}

	/**
	 * Returns supplier for reading data from this storage. It read it from external memory,
	 * decompresses and deserializes it
	 *
	 * @param partition index of partition to read
	 */
	@Override
	public Promise<StreamSupplier<T>> read(int partition) {
		Path path = partitionPath(partition);

		return ChannelFileReader.open(executor, path)
				.map(file -> file
						.transformWith(ChannelFrameDecoder.create(frameFormat))
						.transformWith(ChannelDeserializer.create(serializer)));
	}

	/**
	 * Method which removes all creating files
	 */
	@Override
	public Promise<Void> cleanup(List<Integer> partitionsToDelete) {
		return Promise.ofBlocking(executor, () -> {
			for (Integer partitionToDelete : partitionsToDelete) {
				Path path = partitionPath(partitionToDelete);
				try {
					Files.delete(path);
				} catch (IOException e) {
					logger.warn("Could not delete {}", path, e);
				}
			}
		});
	}
}
