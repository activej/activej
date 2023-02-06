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

package io.activej.datastream.processor.transformer.sort;

import io.activej.common.MemSize;
import io.activej.common.builder.AbstractBuilder;
import io.activej.csp.file.ChannelFileReader;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.csp.process.ChannelByteChunker;
import io.activej.csp.process.frame.ChannelFrameDecoder;
import io.activej.csp.process.frame.ChannelFrameEncoder;
import io.activej.csp.process.frame.FrameFormat;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.StreamConsumers;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.reactor.ImplicitlyReactive;
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
public final class StreamSorterStorage<T> extends ImplicitlyReactive
		implements IStreamSorterStorage<T> {
	private static final Logger logger = LoggerFactory.getLogger(StreamSorterStorage.class);

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

	private StreamSorterStorage(Executor executor, BinarySerializer<T> serializer,
			FrameFormat frameFormat, Path path) {
		this.executor = executor;
		this.serializer = serializer;
		this.frameFormat = frameFormat;
		this.path = path;
	}

	/**
	 * Creates a new stream sorter storage
	 *
	 * @param executor   executor for running tasks in new thread
	 * @param serializer for serialization to bytes
	 * @param path       path in which will store received data
	 */
	public static <T> StreamSorterStorage<T> create(Executor executor,
			BinarySerializer<T> serializer, FrameFormat frameFormat, Path path) {
		return StreamSorterStorage.builder(executor, serializer, frameFormat, path).build();
	}

	/**
	 * Creates a builder for stream sorter storage
	 *
	 * @param executor   executor for running tasks in new thread
	 * @param serializer for serialization to bytes
	 * @param path       path in which will store received data
	 */
	public static <T> StreamSorterStorage<T>.Builder builder(Executor executor,
			BinarySerializer<T> serializer, FrameFormat frameFormat, Path path) {
		checkArgument(!path.getFileName().toString().contains("%d"), "Filename should not contain '%d'");
		try {
			Files.createDirectories(path);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		return new StreamSorterStorage<>(executor, serializer, frameFormat, path).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, StreamSorterStorage<T>> {
		private Builder() {}

		public Builder withFilePattern(String filePattern) {
			checkNotBuilt(this);
			checkArgument(!filePattern.contains("%d"), "File pattern should not contain '%d'");
			StreamSorterStorage.this.filePattern = filePattern;
			return this;
		}

		public Builder withReadBlockSize(MemSize readBlockSize) {
			checkNotBuilt(this);
			StreamSorterStorage.this.readBlockSize = readBlockSize;
			return this;
		}

		public Builder withWriteBlockSize(MemSize writeBlockSize) {
			checkNotBuilt(this);
			StreamSorterStorage.this.writeBlockSize = writeBlockSize;
			return this;
		}

		@Override
		protected StreamSorterStorage<T> doBuild() {
			return StreamSorterStorage.this;
		}
	}

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
		return Promise.of(StreamConsumers.ofSupplier(
				supplier -> supplier
						.transformWith(ChannelSerializer.builder(serializer)
								.withInitialBufferSize(readBlockSize)
								.build())
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
