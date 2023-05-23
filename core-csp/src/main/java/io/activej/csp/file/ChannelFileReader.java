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

package io.activej.csp.file;

import io.activej.async.exception.AsyncCloseException;
import io.activej.async.file.ExecutorFileService;
import io.activej.async.file.IFileService;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.MemSize;
import io.activej.common.builder.AbstractBuilder;
import io.activej.csp.supplier.AbstractChannelSupplier;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executor;

import static io.activej.common.Checks.checkArgument;
import static java.nio.file.StandardOpenOption.READ;

/**
 * This supplier allows you to asynchronously read binary data from a file.
 */
public final class ChannelFileReader extends AbstractChannelSupplier<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(ChannelFileReader.class);

	private static final OpenOption[] DEFAULT_OPTIONS = new OpenOption[]{READ};

	public static final MemSize DEFAULT_BUFFER_SIZE = MemSize.kilobytes(8);

	private final IFileService fileService;
	private final FileChannel channel;

	private int bufferSize = DEFAULT_BUFFER_SIZE.toInt();
	private long position = 0;
	private long limit = Long.MAX_VALUE;

	private ChannelFileReader(IFileService fileService, FileChannel channel) {
		this.fileService = fileService;
		this.channel = channel;
	}

	public static ChannelFileReader create(Reactor reactor, Executor executor, FileChannel channel) {
		return builder(reactor, executor, channel).build();
	}

	public static ChannelFileReader create(IFileService fileService, FileChannel channel) {
		return builder(fileService, channel).build();
	}

	public static Promise<ChannelFileReader> open(Executor executor, Path path) {
		return open(executor, path, DEFAULT_OPTIONS);
	}

	public static Promise<ChannelFileReader> open(Executor executor, Path path, OpenOption... openOptions) {
		return builderOpen(executor, path, openOptions)
			.map(AbstractBuilder::build);
	}

	public static ChannelFileReader openBlocking(Reactor reactor, Executor executor, Path path) throws IOException {
		return openBlocking(reactor, executor, path, DEFAULT_OPTIONS);
	}

	public static ChannelFileReader openBlocking(Reactor reactor, Executor executor, Path path, OpenOption... openOptions) throws IOException {
		return builderBlocking(reactor, executor, path, openOptions).build();
	}

	public static Builder builder(Reactor reactor, Executor executor, FileChannel channel) {
		return builder(new ExecutorFileService(reactor, executor), channel);
	}

	public static Builder builder(IFileService fileService, FileChannel channel) {
		return new ChannelFileReader(fileService, channel).new Builder();
	}

	public static Promise<Builder> builderOpen(Executor executor, Path path) {
		return builderOpen(executor, path, DEFAULT_OPTIONS);
	}

	public static Promise<Builder> builderOpen(Executor executor, Path path, OpenOption... openOptions) {
		checkArgument(List.of(openOptions).contains(READ), "'READ' option is not present");
		return Promise.ofBlocking(executor,
				() -> {
					if (Files.isDirectory(path)) throw new FileSystemException(path.toString(), null, "Is a directory");
					return FileChannel.open(path, openOptions);
				})
			.map(channel -> builder(Reactor.getCurrentReactor(), executor, channel));
	}

	public static Builder builderBlocking(Reactor reactor, Executor executor, Path path) throws IOException {
		return builderBlocking(reactor, executor, path, DEFAULT_OPTIONS);
	}

	public static Builder builderBlocking(Reactor reactor, Executor executor, Path path, OpenOption... openOptions) throws IOException {
		checkArgument(List.of(openOptions).contains(READ), "'READ' option is not present");
		if (Files.isDirectory(path)) throw new FileSystemException(path.toString(), null, "Is a directory");
		FileChannel channel = FileChannel.open(path, openOptions);
		return builder(reactor, executor, channel);
	}

	public final class Builder extends AbstractBuilder<Builder, ChannelFileReader> {
		private Builder() {}

		public Builder withBufferSize(MemSize bufferSize) {
			checkNotBuilt(this);
			return withBufferSize(bufferSize.toInt());
		}

		public Builder withBufferSize(int bufferSize) {
			checkNotBuilt(this);
			checkArgument(bufferSize > 0, "Buffer size cannot be less than or equal to zero");
			ChannelFileReader.this.bufferSize = bufferSize;
			return this;
		}

		public Builder withOffset(long offset) {
			checkNotBuilt(this);
			checkArgument(offset >= 0, "Offset cannot be less than zero");
			position = offset;
			return this;
		}

		public Builder withLimit(long limit) {
			checkNotBuilt(this);
			checkArgument(limit >= 0, "Limit cannot be less than zero");
			ChannelFileReader.this.limit = limit;
			return this;
		}

		@Override
		protected ChannelFileReader doBuild() {
			return ChannelFileReader.this;
		}
	}

	public long getPosition() {
		return position;
	}

	@Override
	protected Promise<ByteBuf> doGet() {
		if (limit == 0) {
			close();
			return Promise.of(null);
		}
		ByteBuf buf = ByteBufPool.allocateExact((int) Math.min(bufferSize, limit));
		return fileService.read(channel, position, buf.array(), buf.head(), buf.writeRemaining())
			.then(
				bytesRead -> {
					if (bytesRead == 0) { // no data read, assuming end of file
						buf.recycle();
						close();
						return Promise.of(null);
					}

					buf.moveTail(Math.toIntExact(bytesRead));
					position += bytesRead;
					if (limit != Long.MAX_VALUE) {
						limit -= bytesRead; // bytesRead is always <= the limit (^ see the min call)
					}
					return Promise.of(buf);
				},
				e -> {
					buf.recycle();
					closeEx(e);
					return Promise.ofException(getException());
				});
	}

	@Override
	protected void onClosed(Exception e) {
		try {
			if (!channel.isOpen()) {
				throw new AsyncCloseException("File has been closed");
			}

			channel.close();
			logger.trace("{}: closed file", this);
		} catch (IOException | AsyncCloseException e1) {
			logger.error("{}: failed to close file", this, e1);
		}
	}

	@Override
	public String toString() {
		return "ChannelFileReader{" +
			"pos=" + position +
			(limit == Long.MAX_VALUE ? "" : ", limit=" + limit) +
			'}';
	}
}
