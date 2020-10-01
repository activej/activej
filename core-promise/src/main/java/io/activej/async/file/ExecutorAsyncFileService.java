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

package io.activej.async.file;

import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Executor;

import static io.activej.promise.Promise.ofBlockingCallable;

public final class ExecutorAsyncFileService implements AsyncFileService {
	private final Executor executor;

	public ExecutorAsyncFileService(@NotNull Executor executor) {
		this.executor = executor;
	}

	@Override
	public Promise<Integer> read(FileChannel channel, long position, byte[] array, int offset, int size) {
		return ofBlockingCallable(executor, () -> {
			ByteBuffer buffer = ByteBuffer.wrap(array, offset, size);
			long pos = position;

			do {
				int readBytes = channel.read(buffer, pos);
				if (readBytes == -1) {
					break;
				}
				pos += readBytes;
			} while (buffer.position() < buffer.limit());
			return Math.toIntExact(pos - position);
		});
	}

	@Override
	public Promise<Integer> write(FileChannel channel, long position, byte[] array, int offset, int size) {
		return ofBlockingCallable(executor, () -> {
			ByteBuffer buffer = ByteBuffer.wrap(array, offset, size);
			long pos = position;

			do {
				pos += channel.write(buffer, pos);
			} while (buffer.position() < buffer.limit());
			return Math.toIntExact(pos - position);
		});
	}
}
