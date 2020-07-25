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
import io.activej.csp.file.ChannelFileReader;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.csp.process.ChannelByteChunker;
import io.activej.csp.process.ChannelLZ4Compressor;
import io.activej.csp.process.ChannelLZ4Decompressor;
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
public final class StreamSorterStorageImpl<T> implements StreamSorterStorage<T> {
	private static final Logger logger = LoggerFactory.getLogger(StreamSorterStorageImpl.class);

	public static final String DEFAULT_FILE_PATTERN = "%d";
	public static final MemSize DEFAULT_SORTER_BLOCK_SIZE = MemSize.kilobytes(256);

	private static final AtomicInteger PARTITION = new AtomicInteger();

	private final Executor executor;
	private final BinarySerializer<T> serializer;
	private final Path path;

	private String filePattern = DEFAULT_FILE_PATTERN;
	private MemSize readBlockSize = ChannelSerializer.DEFAULT_INITIAL_BUFFER_SIZE;
	private MemSize writeBlockSize = DEFAULT_SORTER_BLOCK_SIZE;
	private int compressionLevel = 0;

	// region creators
	private StreamSorterStorageImpl(Executor executor, BinarySerializer<T> serializer,
			Path path) {
		this.executor = executor;
		this.serializer = serializer;
		this.path = path;
	}

	/**
	 * Creates a new storage
	 *
	 * @param executor   executor for running tasks in new thread
	 * @param serializer for serialization to bytes
	 * @param path       path in which will store received data
	 */
	public static <T> StreamSorterStorageImpl<T> create(Executor executor,
			BinarySerializer<T> serializer, Path path) {
		checkArgument(!path.getFileName().toString().contains("%d"), "Filename should not contain '%d'");
		try {
			Files.createDirectories(path);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		return new StreamSorterStorageImpl<>(executor, serializer, path);
	}

	public StreamSorterStorageImpl<T> withFilePattern(String filePattern) {
		checkArgument(!filePattern.contains("%d"), "File pattern should not contain '%d'");
		this.filePattern = filePattern;
		return this;
	}

	public StreamSorterStorageImpl<T> withReadBlockSize(MemSize readBlockSize) {
		this.readBlockSize = readBlockSize;
		return this;
	}

	public StreamSorterStorageImpl<T> withWriteBlockSize(MemSize writeBlockSize) {
		this.writeBlockSize = writeBlockSize;
		return this;
	}

	public StreamSorterStorageImpl<T> withCompressionLevel(int compressionLevel) {
		this.compressionLevel = compressionLevel;
		return this;
	}

	// endregion

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
						.transformWith(ChannelLZ4Compressor.create(compressionLevel))
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
						.transformWith(ChannelLZ4Decompressor.create())
						.transformWith(ChannelDeserializer.create(serializer)));
	}

	/**
	 * Method which removes all creating files
	 */
	@Override
	public Promise<Void> cleanup(List<Integer> partitionsToDelete) {
		return Promise.ofBlockingCallable(executor, () -> {
			for (Integer partitionToDelete : partitionsToDelete) {
				Path path1 = partitionPath(partitionToDelete);
				try {
					Files.delete(path1);
				} catch (IOException e) {
					logger.warn("Could not delete {}", path1, e);
				}
			}
			return null;
		});
	}
}
