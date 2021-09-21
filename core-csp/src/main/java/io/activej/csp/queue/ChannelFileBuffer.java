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

package io.activej.csp.queue;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.MemSize;
import io.activej.common.initializer.WithInitializer;
import io.activej.common.tuple.Tuple2;
import io.activej.csp.file.ChannelFileReader;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executor;

import static java.nio.file.StandardOpenOption.*;

public final class ChannelFileBuffer implements ChannelQueue<ByteBuf>, WithInitializer<ChannelFileBuffer> {
	private static final Logger logger = LoggerFactory.getLogger(ChannelFileBuffer.class);

	private final ChannelFileReader reader;
	private final ChannelFileWriter writer;
	private final Executor executor;
	private final Path path;

	private @Nullable SettablePromise<ByteBuf> take;

	private boolean finished = false;

	private @Nullable Exception exception;

	private ChannelFileBuffer(ChannelFileReader reader, ChannelFileWriter writer, Executor executor, Path path) {
		this.reader = reader;
		this.writer = writer;
		this.executor = executor;
		this.path = path;
	}

	public static Promise<ChannelFileBuffer> create(Executor executor, Path filePath) {
		return create(executor, filePath, null);
	}

	public static Promise<ChannelFileBuffer> create(Executor executor, Path path, @Nullable MemSize limit) {
		return Promise.ofBlocking(executor,
						() -> {
							Files.createDirectories(path.getParent());
							FileChannel writerChannel = FileChannel.open(path, CREATE, WRITE);
							FileChannel readerChannel = FileChannel.open(path, CREATE, READ);
							return new Tuple2<>(writerChannel, readerChannel);
						})
				.map(tuple2 -> {
					ChannelFileWriter writer = ChannelFileWriter.create(executor, tuple2.getValue1());
					ChannelFileReader reader = ChannelFileReader.create(executor, tuple2.getValue2());
					if (limit != null) {
						reader.withLimit(limit.toLong());
					}
					return new ChannelFileBuffer(reader, writer, executor, path);
				});
	}

	@Override
	public Promise<Void> put(@Nullable ByteBuf item) {
		if (exception != null) {
			return Promise.ofException(exception);
		}
		if (item == null) {
			finished = true;
		}
		if (take == null) {
			return writer.accept(item);
		}
		SettablePromise<ByteBuf> promise = take;
		take = null;
		promise.set(item);
		return item == null ?
				writer.accept(null) :
				Promise.complete();
	}

	@Override
	public Promise<ByteBuf> take() {
		if (exception != null) {
			return Promise.ofException(exception);
		}
		if (!isExhausted()) {
			return reader.get();
		}
		if (finished) {
			return Promise.of(null);
		}
		SettablePromise<ByteBuf> promise = new SettablePromise<>();
		take = promise;
		return promise;
	}

	@Override
	public boolean isSaturated() {
		return false;
	}

	@Override
	public boolean isExhausted() {
		return reader.getPosition() >= writer.getPosition();
	}

	@Override
	public void closeEx(@NotNull Exception e) {
		if (exception != null) {
			return;
		}
		exception = e;
		writer.closeEx(e);
		reader.closeEx(e);

		if (take != null) {
			take.setException(e);
			take = null;
		}

		// each queue should operate on files with unique names
		// to avoid races due to this
		executor.execute(() -> {
			try {
				Files.deleteIfExists(path);
			} catch (IOException io) {
				logger.error("failed to cleanup channel buffer file " + path, io);
			}
		});
	}

	public @Nullable Exception getException() {
		return exception;
	}
}
