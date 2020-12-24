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
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Represents the 'predefined storage' for the {@link io.activej.http.StaticServlet StaticServlet}.
 */
public interface StaticLoader {

	Promise<ByteBuf> load(String path);

	default StaticLoader filter(Predicate<String> predicate) {
		return path -> predicate.test(path) ?
				load(path) :
				Promise.ofException(new ResourceNotFoundException("Resource '" + path + "' has been filtered out"));
	}

	default StaticLoader map(Function<String, String> fn) {
		return path -> this.load(fn.apply(path));
	}

	default StaticLoader subdirectory(String subdirectory) {
		String folder = subdirectory.endsWith("/") ? subdirectory : subdirectory + '/';
		return map(name -> folder + name);
	}

	default StaticLoader cached() {
		return cacheOf(this);
	}

	default StaticLoader cached(Map<String, byte[]> map) {
		return cacheOf(this, map);
	}

	static StaticLoader cacheOf(StaticLoader loader) {
		return cacheOf(loader, new HashMap<>());
	}

	static StaticLoader cacheOf(StaticLoader loader, Map<String, byte[]> map) {
		return cacheOf(loader, map::get, map::put);
	}

	static StaticLoader cacheOf(StaticLoader loader, Function<String, byte[]> get, BiConsumer<String, byte[]> put) {
		return new StaticLoaderCache(loader, get, put);
	}

	static StaticLoader ofClassPath(@NotNull Executor executor, String root) {
		return StaticLoaderClassPath.create(executor, root);
	}

	static StaticLoader ofClassPath(@NotNull Executor executor, ClassLoader classLoader, String root) {
		return StaticLoaderClassPath.create(executor, classLoader, root);
	}

	static StaticLoader ofPath(@NotNull Executor executor, Path dir) {
		return StaticLoaderFileReader.create(executor, dir);
	}

}
