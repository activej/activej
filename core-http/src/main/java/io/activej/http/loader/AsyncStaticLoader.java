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
import io.activej.http.Servlet_Static;
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
 * Represents the 'predefined storage' for the {@link Servlet_Static StaticServlet}.
 */
@ComponentInterface
public interface AsyncStaticLoader {

	Promise<ByteBuf> load(String path);

	default AsyncStaticLoader filter(Predicate<String> predicate) {
		return path -> predicate.test(path) ?
				load(path) :
				Promise.ofException(new ResourceNotFoundException("Resource '" + path + "' has been filtered out"));
	}

	default AsyncStaticLoader map(UnaryOperator<String> fn) {
		return path -> this.load(fn.apply(path));
	}

	default AsyncStaticLoader subdirectory(String subdirectory) {
		String folder = subdirectory.endsWith("/") ? subdirectory : subdirectory + '/';
		return map(name -> folder + name);
	}

	default AsyncStaticLoader cached(Reactor reactor) {
		return cacheOf(reactor, this);
	}

	default AsyncStaticLoader cached(Reactor reactor, Map<String, byte[]> map) {
		return cacheOf(reactor, this, map);
	}

	static AsyncStaticLoader cacheOf(Reactor reactor, AsyncStaticLoader loader) {
		return cacheOf(reactor, loader, new HashMap<>());
	}

	static AsyncStaticLoader cacheOf(Reactor reactor, AsyncStaticLoader loader, Map<String, byte[]> map) {
		return cacheOf(reactor, loader, map::get, map::put);
	}

	static AsyncStaticLoader cacheOf(Reactor reactor, AsyncStaticLoader loader, Function<String, byte[]> get, BiConsumer<String, byte[]> put) {
		return new StaticLoader_Cache(reactor, loader, get, put);
	}

	static AsyncStaticLoader ofClassPath(Reactor reactor, Executor executor, String root) {
		return StaticLoader_ClassPath.create(reactor, executor, root);
	}

	static AsyncStaticLoader ofClassPath(Reactor reactor, Executor executor, ClassLoader classLoader, String root) {
		return StaticLoader_ClassPath.create(reactor, executor, classLoader, root);
	}

	static AsyncStaticLoader ofPath(Reactor reactor, Executor executor, Path dir) {
		return StaticLoader_FileReader.create(reactor, executor, dir);
	}

}
