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

package io.activej.http.loader;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.csp.file.ChannelFileReader;
import io.activej.promise.Promise;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executor;

class StaticLoaderFileReader implements AsyncStaticLoader {
	private final Executor executor;
	private final Path root;

	private StaticLoaderFileReader(Executor executor, Path root) {
		this.executor = executor;
		this.root = root;
	}

	public static AsyncStaticLoader create(Executor executor, Path dir) {
		return new StaticLoaderFileReader(executor, dir);
	}

	@Override
	public Promise<ByteBuf> load(String path) {
		Path file = root.resolve(path).normalize();

		if (!file.startsWith(root)) {
			return Promise.ofException(new ResourceNotFoundException("Could not find '" + path + '\''));
		}

		return Promise.ofBlocking(executor,
				() -> {
					if (Files.isRegularFile(file)) {
						return;
					}
					if (Files.isDirectory(file)) {
						throw new ResourceIsADirectoryException("Resource '" + path + "' is a directory");
					} else {
						throw new ResourceNotFoundException("Could not find '" + path + '\'');
					}
				})
				.then(() -> ChannelFileReader.open(executor, file))
				.then(cfr -> cfr.toCollector(ByteBufs.collector()));
	}
}
