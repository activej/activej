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
import io.activej.common.time.CurrentTimeProvider;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelInput;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.process.AbstractCommunicatingProcess;
import io.activej.fs.ActiveFs;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.util.function.UnaryOperator;

final class LogStreamChunker extends AbstractCommunicatingProcess implements ChannelInput<ByteBuf> {
	private final CurrentTimeProvider currentTimeProvider;
	private final ActiveFs fs;
	private final LogNamingScheme namingScheme;
	private final String logPartition;
	private final UnaryOperator<ChannelConsumer<ByteBuf>> consumerTransformer;

	private ChannelSupplier<ByteBuf> input;
	private @Nullable ChannelConsumer<ByteBuf> currentConsumer;

	private LogFile currentChunk;

	public LogStreamChunker(CurrentTimeProvider currentTimeProvider, ActiveFs fs, LogNamingScheme namingScheme, String logPartition, UnaryOperator<ChannelConsumer<ByteBuf>> consumerTransformer) {
		this.currentTimeProvider = currentTimeProvider;
		this.fs = fs;
		this.namingScheme = namingScheme;
		this.logPartition = logPartition;
		this.consumerTransformer = consumerTransformer;
	}

	@Override
	public Promise<Void> set(ChannelSupplier<ByteBuf> input) {
		this.input = sanitize(input);
		startProcess();
		return getProcessCompletion();
	}

	@Override
	protected void doProcess() {
		input.get()
				.whenResult(buf -> {
					if (buf != null) {
						//noinspection ConstantConditions
						ensureConsumer()
								.then(() -> currentConsumer.accept(buf))
								.whenResult(this::doProcess);
					} else {
						flush().whenResult(this::completeProcess);
					}
				});
	}

	private Promise<Void> ensureConsumer() {
		LogFile newChunkName = namingScheme.format(currentTimeProvider.currentTimeMillis());
		return currentChunk != null && currentChunk.getName().compareTo(newChunkName.getName()) >= 0 ?
				Promise.complete() :
				startNewChunk(newChunkName);
	}

	private Promise<Void> startNewChunk(LogFile newChunkName) {
		return flush()
				.then(() -> {
					this.currentChunk = (currentChunk == null) ? newChunkName : new LogFile(newChunkName.getName(), 0);
					return fs.append(namingScheme.path(logPartition, currentChunk), 0)
							.then(this::doSanitize)
							.whenResult(newConsumer -> this.currentConsumer = consumerTransformer.apply(sanitize(newConsumer)))
							.toVoid();
				});
	}

	private Promise<Void> flush() {
		if (currentConsumer == null) {
			return Promise.complete();
		}
		return currentConsumer.acceptEndOfStream()
				.whenResult(() -> currentConsumer = null);
	}

	@Override
	protected void doClose(Exception e) {
		input.closeEx(e);
		if (currentConsumer != null) {
			currentConsumer.closeEx(e);
		}
	}
}
