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

import io.activej.async.file.AsyncFileService;
import io.activej.async.file.ExecutorAsyncFileService;
import io.activej.bytebuf.ByteBuf;
import io.activej.csp.AbstractChannelConsumer;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.Executor;

import static io.activej.common.Checks.checkArgument;
import static java.nio.file.StandardOpenOption.*;

/**
 * This consumer allows you to asynchronously write binary data to a file.
 */
public final class ChannelFileWriter extends AbstractChannelConsumer<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(ChannelFileWriter.class);

	private static final OpenOption[] DEFAULT_OPTIONS = new OpenOption[]{WRITE, CREATE_NEW, APPEND};

	private final AsyncFileService fileService;
	private final FileChannel channel;

	private boolean forceOnClose = false;
	private boolean forceMetadata = false;
	private long startingOffset = 0;
	private boolean started;

	private long position = 0;

	private ChannelFileWriter(AsyncFileService fileService, FileChannel channel) {
		this.fileService = fileService;
		this.channel = channel;
	}

	public static ChannelFileWriter create(Executor executor, FileChannel channel) {
		return create(new ExecutorAsyncFileService(executor), channel);
	}

	public static ChannelFileWriter create(AsyncFileService fileService, FileChannel channel) {
		return new ChannelFileWriter(fileService, channel);
	}

	public static Promise<ChannelFileWriter> open(Executor executor, Path path) {
		return open(executor, path, DEFAULT_OPTIONS);
	}

	public static Promise<ChannelFileWriter> open(Executor executor, Path path, OpenOption... openOptions) {
		checkArgument(Arrays.asList(openOptions).contains(WRITE), "'WRITE' option is not present");
		return Promise.ofBlockingCallable(executor, () -> FileChannel.open(path, openOptions))
				.map(channel -> create(executor, channel));
	}

	public static ChannelFileWriter openBlocking(Executor executor, Path path) throws IOException {
		return openBlocking(executor, path, DEFAULT_OPTIONS);
	}

	public static ChannelFileWriter openBlocking(Executor executor, Path path, OpenOption... openOptions) throws IOException {
		checkArgument(Arrays.asList(openOptions).contains(WRITE), "'WRITE' option is not present");
		FileChannel channel = FileChannel.open(path, openOptions);
		return create(executor, channel);
	}

	public ChannelFileWriter withForceOnClose(boolean forceMetadata) {
		forceOnClose = true;
		this.forceMetadata = forceMetadata;
		return this;
	}

	public ChannelFileWriter withOffset(long offset) {
		startingOffset = offset;
		return this;
	}

	public long getPosition() {
		return position;
	}

	@Override
	protected void onClosed(@NotNull Throwable e) {
		try {
			closeFile();
		} catch (IOException ex) {
			logger.error("{}: failed to close file", this, ex);
		}
	}

	@Override
	protected Promise<Void> doAccept(ByteBuf buf) {
		if (!started) {
			position = startingOffset;
		}
		started = true;
		if (buf == null) {
			try {
				closeFile();
			} catch (IOException e) {
				return Promise.ofException(e);
			}
			close();
			return Promise.of(null);
		}
		long p = position;
		position += buf.readRemaining();

		byte[] array = buf.getArray();
		return fileService.write(channel, p, array, 0, array.length)
				.thenEx(($, e2) -> {
					if (isClosed()) return Promise.ofException(getException());
					if (e2 != null) {
						closeEx(e2);
					}
					return Promise.of($, e2);
				})
				.then(() -> {
					buf.recycle();
					return Promise.complete();
				});
	}

	private void closeFile() throws IOException {
		if (!channel.isOpen()) {
			return;
		}

		if (forceOnClose) {
			channel.force(forceMetadata);
		}

		channel.close();
		logger.trace("{}: closed file", this);
	}

	@Override
	public String toString() {
		return "ChannelFileWriter{pos=" + position + '}';
	}
}
