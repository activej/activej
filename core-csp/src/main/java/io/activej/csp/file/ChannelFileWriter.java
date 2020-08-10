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
import io.activej.common.exception.AsyncTimeoutException;
import io.activej.csp.AbstractChannelConsumer;
import io.activej.eventloop.schedule.ScheduledRunnable;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Executor;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.nullify;
import static io.activej.eventloop.util.RunnableWithContext.wrapContext;
import static java.nio.file.StandardOpenOption.*;

/**
 * This consumer allows you to asynchronously write binary data to a file.
 */
public final class ChannelFileWriter extends AbstractChannelConsumer<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(ChannelFileWriter.class);

	public static final AsyncTimeoutException IDLE_TIMEOUT_EXCEPTION = new AsyncTimeoutException(ChannelFileWriter.class,
			"Writer was idle for too long");
	public static final int NO_TIMEOUT = 0;

	private static final OpenOption[] DEFAULT_OPTIONS = new OpenOption[]{WRITE, CREATE_NEW, APPEND};

	private final AsyncFileService fileService;
	private final FileChannel channel;

	private boolean forceOnClose = false;
	private boolean forceMetadata = false;
	private long startingOffset = 0;
	private boolean started;
	private int idleTimeout = NO_TIMEOUT;

	private long position = 0;

	@Nullable
	private ScheduledRunnable scheduledIdleTimeout;

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

	public ChannelFileWriter withIdleTimeout(Duration idleTimeout) {
		this.idleTimeout = Math.toIntExact(idleTimeout.toMillis());
		if (this.idleTimeout != NO_TIMEOUT) {
			scheduleIdleTimeout();
		}
		return this;
	}

	public long getPosition() {
		return position;
	}

	@Override
	protected void onClosed(@NotNull Throwable e) {
		scheduledIdleTimeout = nullify(scheduledIdleTimeout, ScheduledRunnable::cancel);
		closeFile();
	}

	@Override
	protected Promise<Void> doAccept(ByteBuf buf) {
		scheduledIdleTimeout = nullify(scheduledIdleTimeout, ScheduledRunnable::cancel);
		if (!started) {
			position = startingOffset;
		}
		started = true;
		if (buf == null) {
			closeFile();
			close();
			return Promise.of(null);
		}
		long p = position;
		position += buf.readRemaining();

		byte[] array = buf.getArray();
		return fileService.write(channel, p, array, 0, array.length)
				.thenEx(($, e) -> {
					if (isClosed()) return Promise.ofException(getException());
					if (e != null) {
						closeEx(e);
					}
					buf.recycle();
					if (idleTimeout != NO_TIMEOUT) {
						scheduleIdleTimeout();
					}
					return Promise.complete();
				});
	}

	private void closeFile() {
		if (!channel.isOpen()) {
			return;
		}

		try {
			if (forceOnClose) {
				channel.force(forceMetadata);
			}

			channel.close();
			logger.trace("{}: closed file", this);
		} catch (IOException e) {
			logger.error("{}: failed to close file", this, e);
		}
	}

	private void scheduleIdleTimeout() {
		assert idleTimeout != NO_TIMEOUT;
		scheduledIdleTimeout = eventloop.delayBackground(idleTimeout, wrapContext(this, () -> {
			scheduledIdleTimeout = null;
			closeEx(IDLE_TIMEOUT_EXCEPTION);
		}));
	}

	@Override
	public String toString() {
		return "ChannelFileWriter{pos=" + position + '}';
	}
}
