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
import io.activej.common.annotation.ComponentInterface;
import io.activej.http.StaticServlet;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Represents the 'predefined storage' for the {@link StaticServlet StaticServlet}.
 */
@ComponentInterface
public interface IStaticLoader {

	Promise<ByteBuf> load(String path);

	default IStaticLoader filter(Predicate<String> predicate) {
		return path -> predicate.test(path) ?
				load(path) :
				Promise.ofException(new ResourceNotFoundException("Resource '" + path + "' has been filtered out"));
	}

	default IStaticLoader map(UnaryOperator<String> fn) {
		return path -> this.load(fn.apply(path));
	}

	default IStaticLoader subdirectory(String subdirectory) {
		String folder = subdirectory.endsWith("/") ? subdirectory : subdirectory + '/';
		return map(name -> folder + name);
	}

	default IStaticLoader cached(Reactor reactor) {
		return cacheOf(reactor, this);
	}

	default IStaticLoader cached(Reactor reactor, Map<String, byte[]> map) {
		return cacheOf(reactor, this, map);
	}

	static IStaticLoader cacheOf(Reactor reactor, IStaticLoader loader) {
		return cacheOf(reactor, loader, new HashMap<>());
	}

	static IStaticLoader cacheOf(Reactor reactor, IStaticLoader loader, Map<String, byte[]> map) {
		return cacheOf(reactor, loader, map::get, map::put);
	}

	static IStaticLoader cacheOf(Reactor reactor, IStaticLoader loader, Function<String, byte[]> get, BiConsumer<String, byte[]> put) {
		return new CacheStaticLoader(reactor, loader, get, put);
	}

	static IStaticLoader ofClassPath(Reactor reactor, Executor executor, String root) {
		return ClassPathStaticLoader.create(reactor, executor, root);
	}

	static IStaticLoader ofClassPath(Reactor reactor, Executor executor, ClassLoader classLoader, String root) {
		return ClassPathStaticLoader.create(reactor, executor, classLoader, root);
	}

	static IStaticLoader ofPath(Reactor reactor, Executor executor, Path dir) {
		return new FileReaderStaticLoader(reactor, executor, dir);
	}

}
